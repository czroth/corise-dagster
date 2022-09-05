from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    sensor,
    static_partitioned_config,
)
from project.resources import mock_s3_resource, redis_resource, s3_resource
from project.sensors import get_s3_keys
from project.types import Aggregation, Stock


@op(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context) -> [Stock]:
    output = list()
    for row in context.resources.s3.get_data(context.op_config["s3_key"]):
        print(row)
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"agg_max": Out(dagster_type=Aggregation)},
    tags={"kind": "bi"},
    description="Find the highest stock price and date",
)
def process_data(stocks: List[Stock]) -> Aggregation:
    return Aggregation(
        date=(highest_stock := max(stocks, key=lambda stock: stock.high)).date,
        high=highest_stock.high,
    )


@op(
    required_resource_keys={"redis"},
    ins={"agg_max": In(dagster_type=Aggregation)},
    out=None,
    tags={"kind": "redis"},
    description="Post aggregate result to Redis",
)
def put_redis_data(context, agg_max: Aggregation) -> None:
    context.log.debug(f"Putting {agg_max} to Redis")
    context.resources.redis.put_data(
        name=f"{agg_max.date}",
        value=agg_max.high,
    )


@graph
def week_3_pipeline():
    put_redis_data(process_data(get_s3_data()))


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://host.docker.internal:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


def docker_config():
    pass


local_week_3_pipeline = week_3_pipeline.to_job(
    name="local_week_3_pipeline",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
    },
)

docker_week_3_pipeline = week_3_pipeline.to_job(
    name="docker_week_3_pipeline",
    config=docker_config,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
)


local_week_3_schedule = ScheduleDefinition(
    job=local_week_3_pipeline,
    cron_schedule="*/15 * * * *",  # every 15 minutes
)

docker_week_3_schedule = ScheduleDefinition(
    job=docker_week_3_pipeline,
    cron_schedule="0 * * * *",  # every hour
)


@sensor
def docker_week_3_sensor():
    pass
