import os
import argparse
import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)

BUCKET = os.getenv('S3_BUCKET', 's3-ds-source')
MAX_HOURS = 24

def weekday_name(date_col):
    d = F.dayofweek(date_col)
    return (
        F.when(d == 1, 'воскресенье')
        .when(d == 2, 'понедельник')
        .when(d == 3, 'вторник')
        .when(d == 4, 'среда')
        .when(d == 5, 'четверг')
        .when(d == 6, 'пятница')
        .when(d == 7, 'суббота')
    )


def spark_session():
    b = (
        SparkSession.builder.appName('sng-book-recsys-etl')
        .config('spark.hadoop.fs.s3a.endpoint', os.getenv('S3_ENDPOINT_URL', 'http://s3mock:9090'))
        .config('spark.hadoop.fs.s3a.access.key', os.getenv('AWS_ACCESS_KEY_ID', 'localminio'))
        .config('spark.hadoop.fs.s3a.secret.key', os.getenv('AWS_SECRET_ACCESS_KEY', 'localminio123'))
        .config('spark.hadoop.fs.s3a.path.style.access', 'true')
        # .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

        .config('spark.executor.memory', '2g')
        .config('spark.executor.cores', '2')
        .config('spark.executor.instances', '2')
        .config('spark.driver.cores', '4')
        .config('spark.sql.autoBroadcastJoinThreshold', '-1')
        .config('spark.sql.shuffle.partitions', '32')
        .config('spark.network.timeout', '300')
    )
    return b.getOrCreate()


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--start_date', default=None)
    p.add_argument('--end_date', default=None)
    p.add_argument('--geo_id', default=None)
    p.add_argument('--audition_path', default=f's3a://{BUCKET}/audition.parquet')
    p.add_argument('--content_path', default=f's3a://{BUCKET}/content.parquet')
    p.add_argument('--output_path', default=f's3a://{BUCKET}/results/S24_project_data')
    p.add_argument('--columns', nargs='*', default=None, help='аналитика по колонкам, напр. usage_platform_ru genre')
    p.add_argument('--show-analytics', action='store_true', help='включить аналитику по geo, дням недели, hours')
    p.add_argument('--show-plan', action='store_true', help='распечатать план выполнения построения витрины user_mart')
    p.add_argument('--verbose', action='store_true', help='show/count для отладки и README')
    return p.parse_args()


def filter_audition(df, start_date, end_date, geo_id):
    filter_applied = False
    if start_date:
        filter_applied = True
        df = df.filter(col('msk_business_dt_str') >= start_date)
    if end_date:
        filter_applied = True
        df = df.filter(col('msk_business_dt_str') <= end_date)
    if geo_id:
        filter_applied = True
        df = df.filter(col('usage_geo_id') == geo_id)
    return (df, filter_applied)


def check_nulls(df, name):
    total_rows = df.count()
    if total_rows == 0: return

    agg_exprs = [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]
    stats_row = df.select(agg_exprs).collect()[0]

    for c in df.columns:
        cnt = stats_row[c] or 0
        if cnt > 0:
            pct = (cnt / total_rows) * 100
            logger.info(f"[{name}] пропусков в {c}: {cnt} ({pct:.2f}%)")


def valid_audition_hours():
    return (
        col('hours').isNotNull()
        & col('hours_sessions_long').isNotNull()
        & (col('hours') >= 0) & (col('hours') <= MAX_HOURS)
        & (col('hours_sessions_long') >= 0) & (col('hours_sessions_long') <= MAX_HOURS)
    )


def check_content_duration(con):
    null_dur = con.filter(col('main_content_duration_hours').isNull()).count()
    le_zero_dur = con.filter(
        col('main_content_duration_hours').isNotNull()
        & (col('main_content_duration_hours') <= 0)
    ).count()
    logger.info(
        '[content] до очистки: пропусков main_content_duration_hours=%d, длительность <= 0: %d',
        null_dur,
        le_zero_dur,
    )


def check_data_quality(aud, con, verbose=False):
    if not verbose:
        return

    check_nulls(aud, 'audition')
    check_nulls(con, 'content')

    bad = aud.filter(~valid_audition_hours()).count()
    if bad:
        logger.warning('[audition]: %s строк с невалидными hours/hours_sessions_long (0..%s)', bad, MAX_HOURS)

    dup_aud = aud.groupBy('audition_id').count().filter(col('count') > 1).limit(5).count()
    dup_con = con.groupBy('main_content_id').count().filter(col('count') > 1).limit(5).count()
    if dup_aud:
        logger.warning('[audition]: найдены дубликаты audition_id')
    if dup_con:
        logger.warning('[content]: найдены дубликаты main_content_id')

def clean_audition(aud, verbose=False):
    if verbose:
        before = aud.count()
    aud = aud.filter(valid_audition_hours())
    if verbose:
        dropped = before - aud.count()
        if dropped:
            logger.info('audition: удалено %s строк с невалидными значениями', dropped)
    return aud.fillna({'app_version': 'unknown'})


def clean_content(con, verbose=False):
    median = con.filter(col('main_content_duration_hours') > 0).approxQuantile(
        'main_content_duration_hours', [0.5], 0.01
    )[0]
    if verbose:
        to_fix = con.filter(
            col('main_content_duration_hours').isNull()
            | (col('main_content_duration_hours') <= 0)
        ).count()
        if to_fix:
            logger.info('[content]: %s строк с невалидной длительностью (заменены на медиану %.7f)', to_fix, median)
    return con.withColumn(
        'main_content_duration_hours',
        F.when(col('main_content_duration_hours') <= 0, F.lit(None))
        .otherwise(col('main_content_duration_hours')),
    ).fillna({'main_content_duration_hours': median})


def check_join(aud, joined, verbose=False):
    if not verbose:
        return

    aud_n = aud.count()
    joined_n = joined.count()
    dropped = aud_n - joined_n

    logger.info('[join] audition до join: %s', aud_n)
    logger.info('[join] после inner join: %s', joined_n)

    if joined_n > aud_n:
        raise ValueError(f'join размножил строки на {joined_n - aud_n}')

    if dropped:
        pct = dropped / aud_n * 100
        logger.info(
            '[join] выпало %s сессий (%.2f%%) - main_content_id нет в content, inner join их отрезал',
            dropped, pct,
        )

    logger.info('== Sample joined: ==')
    joined.show(5, truncate=True)


def build_user_content_mart(df, verbose=False):
    dt = col('msk_business_dt_str')
    df = (
        df.groupBy('puid', 'main_content_id')
        .agg(
            F.sum('hours').alias('total_hours'),
            F.count('*').alias('sessions_count'),
            F.max(dt).alias('last_listening_date'),
            (F.datediff(F.max(dt), F.min(dt)) + 1).alias('days_spent'),
            F.first('main_content_duration_hours').alias('main_content_duration_hours'),
        )
        .withColumn(
            'finished_percent',
            F.least(col('total_hours') / col('main_content_duration_hours'), F.lit(1.0)),
        )
        .withColumn('is_completed', F.when(col('finished_percent') >= 1, 1).otherwise(0))
        .withColumn('avg_session_hours', col('total_hours') / col('sessions_count'))
    )
    user_content_mart = df.select(
        'puid',
        'main_content_id',
        'total_hours',
        'sessions_count',
        'finished_percent',
        'is_completed',
        'last_listening_date',
        'days_spent',
        'avg_session_hours',
    )
    if verbose:
        print('\n== user_content_mart ==')
        user_content_mart.show(5, truncate=False)
    return user_content_mart


def build_user_genre_mart(df, user_content_mart, verbose=False):
    df = (
        df.select(
            'puid',
            'main_content_id',
            'hours',
            'adult_content_flg',
            'kids_content_flg',
            F.explode('published_topic_title_list').alias('genre'),
        )
        .filter(col('genre').isNotNull() & (col('genre') != ''))
        .join(
            user_content_mart.select('puid', 'main_content_id', 'is_completed'),
            on=['puid', 'main_content_id'],
        )
        .groupBy('puid', 'genre')
        .agg(
            F.sum('hours').alias('total_hours'),
            F.count('*').alias('sessions_count'),
            F.countDistinct(F.when(col('is_completed') == 1, col('main_content_id'))).alias('completed_count'),
            F.countDistinct(F.when(col('adult_content_flg'), col('main_content_id'))).alias('adult_count'),
            F.countDistinct(F.when(col('kids_content_flg'), col('main_content_id'))).alias('kids_count'),
        )
        .withColumn(
            'genre_rank',
            F.row_number().over(Window.partitionBy('puid').orderBy(F.desc('total_hours'))),
        )
    )
    user_genre_mart = df.select(
        'puid',
        'genre',
        'total_hours',
        'sessions_count',
        'completed_count',
        'genre_rank',
        'adult_count',
        'kids_count',
    )
    if verbose:
        print('\n== user_genre_mart ==')
        user_genre_mart.show(5, truncate=False)
    return user_genre_mart


def top_by_hours(df, dim, out_col):
    w = Window.partitionBy('puid').orderBy(F.desc('h'))
    return (
        df.groupBy('puid', dim)
        .agg(F.sum('hours').alias('h'))
        .withColumn('rn', F.row_number().over(w))
        .filter(col('rn') == 1)
        .select('puid', col(dim).alias(out_col))
    )


def build_user_mart(df, user_content_mart, user_genre_mart, verbose=False):
    dt = col('msk_business_dt_str')
    base = df.groupBy('puid').agg(
        F.sum('hours').alias('total_hours'),
        F.countDistinct(dt).alias('active_days'),
        F.countDistinct('main_content_id').alias('unique_content_count'),
        F.max(dt).alias('last_date'),
        F.countDistinct(F.when(col('adult_content_flg'), col('main_content_id'))).alias('adult_content_count'),
        F.countDistinct(F.when(col('kids_content_flg'), col('main_content_id'))).alias('kids_content_count'),
    )
    genres_count = (
        df.select('puid', F.explode('published_topic_title_list').alias('genre'))
        .filter(col('genre') != '')
        .groupBy('puid')
        .agg(F.countDistinct('genre').alias('unique_genres_count'))
    )
    completion = user_content_mart.groupBy('puid').agg(F.avg('finished_percent').alias('completion_rate'))
    fav_genre = (
        user_genre_mart.filter(col('genre_rank') == 1)
        .select('puid', col('genre').alias('favorite_genre'))
    )
    user_mart = (
        base.join(genres_count, 'puid', 'left')
        .join(completion, 'puid', 'left')
        .join(fav_genre, 'puid', 'left')
        .join(top_by_hours(df, 'main_content_type', 'favorite_content_type'), 'puid', 'left')
        .join(top_by_hours(df, 'usage_platform_ru', 'favourite_platform'), 'puid', 'left')
        .select(
            'puid',
            'total_hours',
            'active_days',
            'unique_content_count',
            'unique_genres_count',
            'completion_rate',
            'favorite_genre',
            'favorite_content_type',
            'favourite_platform',
            'last_date',
            'adult_content_count',
            'kids_content_count',
        )
    )
    if verbose:
        print('\n== user_mart ==')
        user_mart.show(5, truncate=False)
    return user_mart

def build_content_mart(df, user_content_mart, verbose=False):
    n_users = df.select('puid').distinct().count()
    avg_fin = user_content_mart.groupBy('main_content_id').agg(
        (F.avg('finished_percent') * 100).alias('avg_finished_percent')
    )
    content_mart = (
        df.groupBy('main_content_id')
        .agg(
            F.countDistinct('puid').alias('unique_users'),
            F.sum('hours').alias('total_hours'),
            F.countDistinct('usage_platform_ru').alias('platforms_count'),
        )
        .join(avg_fin, 'main_content_id')
        .withColumn('popularity', col('unique_users') / F.lit(n_users))
        .select(
            'main_content_id',
            'unique_users',
            'total_hours',
            'avg_finished_percent',
            'platforms_count',
            'popularity',
        )
    )
    if verbose:
        print('\n== content_mart ==')
        content_mart.show(5, truncate=False)
    return content_mart


def build_daily_user_mart(df, verbose=False):
    daily_user_mart = (
        df.withColumn('session_date', col('msk_business_dt_str'))
        .groupBy('puid', 'session_date')
        .agg(
            F.countDistinct('main_content_id').alias('content_count'),
            F.sum('hours').alias('hours_spent'),
            F.count('*').alias('sessions_count'),
        )
        .withColumn('day_of_week', weekday_name(col('session_date')))
        .select(
            'puid',
            'session_date',
            'day_of_week',
            'content_count',
            'hours_spent',
            'sessions_count',
        )
    )
    if verbose:
        print('\n== daily_user_mart ==')
        daily_user_mart.show(5, truncate=False)
    return daily_user_mart


def make_marts(joined, verbose=False, save_before_content=None):
    uc = build_user_content_mart(joined, verbose=verbose)
    ug = build_user_genre_mart(joined, uc, verbose=verbose)
    um = build_user_mart(joined, uc, ug, verbose=verbose)

    if save_before_content:
        save_marts(
            {'user_content_mart': uc, 'user_genre_mart': ug, 'user_mart': um},
            save_before_content,
        )

    return {
        'user_content_mart': uc,
        'user_genre_mart': ug,
        'user_mart': um,
        'content_mart': build_content_mart(joined, uc, verbose=verbose),
        'daily_user_mart': build_daily_user_mart(joined, verbose=verbose),
    }


def save_marts(marts, output_path, only=None):
    output_path = output_path.rstrip('/')
    for name, df in marts.items():
        if only is not None and name not in only:
            continue
        path = f'{output_path}/{name}.parquet'
        df.write.mode('overwrite').parquet(path)
        logger.info('wrote %s', path)

def distribution_by_column(joined, column):
    return (
        joined.filter(col(column).isNotNull())
        .groupBy(column)
        .agg(
            F.countDistinct('puid').alias('users'),
            F.count('*').alias('sessions'),
        )
        .orderBy(F.desc('users'))
    )


def distribution_by_genre(joined):
    return (
        joined.select('puid', F.explode('published_topic_title_list').alias('genre'))
        .filter(col('genre').isNotNull() & (col('genre') != ''))
        .groupBy('genre')
        .agg(
            F.countDistinct('puid').alias('users'),
            F.count('*').alias('sessions'),
        )
        .orderBy(F.desc('users'))
    )


def show_columns(joined, columns):
    for column in columns:
        logger.info('== распределение: %s ==', column)
        if column == 'genre':
            distribution_by_genre(joined).show(20, truncate=False)
        else:
            distribution_by_column(joined, column).show(20, truncate=False)

def show_analytics(joined):
    logger.info('== аналитика: типы контента ==')
    (
        joined.groupBy('main_content_type')
        .agg(
            F.count('*').alias('sessions'),
            F.countDistinct('puid').alias('users'),
        )
        .orderBy(F.desc('sessions'))
        .show(truncate=False)
    )

    logger.info('== аналитика: hours по дню недели ==')
    (
        joined.withColumn('day_of_week', weekday_name(col('msk_business_dt_str')))
        .groupBy('day_of_week')
        .agg(
            F.round(F.avg('hours'), 4).alias('avg_hours'),
            F.round(F.expr('percentile_approx(hours, 0.5)'), 4).alias('median_hours'),
            F.count('*').alias('sessions'),
        )
        .orderBy('day_of_week')
        .show(truncate=False)
    )

    logger.info('== аналитика: hours по geo (top 15) ==')
    (
        joined.filter(col('usage_geo_id').isNotNull())
        .groupBy('usage_geo_id')
        .agg(
            F.round(F.avg('hours'), 4).alias('avg_hours'),
            F.count('*').alias('sessions'),
        )
        .orderBy(F.desc('sessions'))
        .show(15, truncate=False)
    )


def main():
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    args = parse_args()
    spark = spark_session()
    spark.sparkContext.setLogLevel('WARN')

    verbose = args.verbose

    logger.info('== Загрузка данных ==')
    aud = spark.read.parquet(args.audition_path)
    con = spark.read.parquet(args.content_path)
    if verbose:
        logger.info(
            'Размеры данных:\n'
            f'audition: ({aud.count()}, {len(aud.columns)})\n'
            f'content: ({con.count()}, {len(con.columns)})'
        )
        logger.info('== Схема audition: ==')
        aud.printSchema()
        logger.info('== Sample audition: ==')
        aud.show(5, truncate=False)
        logger.info('== Схема content: ==')
        con.printSchema()
        logger.info('== Sample content: ==')
        con.show(5, truncate=False)

    aud = aud.withColumn('msk_business_dt_str', F.to_date(col('msk_business_dt_str')))

    aud, filter_applied = filter_audition(aud, args.start_date, args.end_date, args.geo_id)
    if filter_applied and verbose:
        logger.info('== Фильтрация audition по дате и гео ==')
        logger.info(f'Размеры audition после фильтрации: ({aud.count()}, {len(aud.columns)})')

    logger.info('== Очистка данных ==')
    check_content_duration(con)
    check_data_quality(aud, con, verbose=verbose)
    aud = clean_audition(aud, verbose=verbose)
    con = clean_content(con, verbose=verbose)

    logger.info('== Объединение данных ==')
    joined = aud.join(con, 'main_content_id', 'inner')

    # Убираем кавычки из списка жанров и преобразуем в список
    joined = joined.withColumn(
        'published_topic_title_list',
        F.transform(
            F.split(col('published_topic_title_list'), ', '),
            lambda g: F.regexp_replace(F.trim(g), '^\'|\'$', ''),
        ),
    )
    check_join(aud, joined, verbose=verbose)

    if args.columns:
        show_columns(joined, args.columns)
    if args.show_analytics:
        show_analytics(joined)

    logger.info('== Построение витрин ==')
    save_early = bool(args.output_path) and not args.show_plan
    marts = make_marts(
        joined,
        verbose=verbose,
        save_before_content=args.output_path if save_early else None,
    )
    if args.show_plan:
        logger.info('== план выполнения user_mart ==')
        marts['user_mart'].explain(mode='extended')

    if args.output_path:
        if save_early:
            save_marts(marts, args.output_path, only={'content_mart', 'daily_user_mart'})
        else:
            save_marts(marts, args.output_path)

    logger.info('== stop spark ==')
    spark.stop()
    logger.info('== finished ==')


if __name__ == '__main__':
    main()
