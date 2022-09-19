from typing import List

from dagster import asset, with_resources
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(
    group_name="corise",
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    op_tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context) -> List[Stock]:
    output = list()
    for row in context.resources.s3.get_data(context.op_config["s3_key"]):
        print(row)
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@asset(
    group_name="corise",
    op_tags={"kind": "bi"},
    description="Find the highest stock price and date",
)
def process_data(get_s3_data: List[Stock]) -> Aggregation:
    return Aggregation(
        date=(highest_stock := max(get_s3_data, key=lambda stock: stock.high)).date,
        high=highest_stock.high,
    )


@asset(
    group_name="corise",
    required_resource_keys={"redis"},
    op_tags={"kind": "redis"},
    description="Post aggregate result to Redis",
)
def put_redis_data(context, process_data: Aggregation) -> None:
    context.log.debug(f"Putting {process_data} to Redis")
    context.resources.redis.put_data(
        name=f"{process_data.date}",
        value=process_data.high,
    )


get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data],
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    resource_config_by_key={
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
                "port": 6379
            },
        },
    },
)