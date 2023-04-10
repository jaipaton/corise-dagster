import csv
from datetime import datetime
from typing import Iterator, List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    String,
    job,
    op,
    usable_as_dagster_type,
)
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[str]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)

@op(config_schema={"s3_key": str}, 
    out={"stock_list": Out(dagster_type=List[Stock], description="Log of Stocks")})
def get_s3_data_op(context):
    s3_key_read = context.op_config['s3_key']
    stock_list = [i for i in csv_helper(s3_key_read)]
    return stock_list

@op(ins={'stock_list': In(dagster_type=List, description='List of Stock elements')},
    out={'highest_val_agg': Out(dagster_type=Aggregation, description='Stock log with highest daily value')})
def process_data_op(stock_list):

    high_val = max(s.high for s in stock_list )
    high_stock = list(filter(lambda stock: stock.high == high_val, stock_list))[0]
    highest_val_agg = Aggregation(date=high_stock.date, high=high_stock.high)
    return highest_val_agg

@op(ins={'highest_val_agg': In(dagster_type=Aggregation, description='Stock log with highest daily value - output of process_data_op')})
def put_redis_data_op(context,highest_val_agg):
    pass


@op(ins={'highest_val_agg': In(dagster_type=Aggregation, description='Stock log with highest daily value - output of process_data_op')})
def put_s3_data_op(context, highest_val_agg):
    pass


@job
def machine_learning_job():
    s3_input = get_s3_data_op()
    highest_val_aggregation = process_data_op(s3_input)
    write_to_redis = put_redis_data_op(highest_val_aggregation)
    write_to_s3 = put_s3_data_op(highest_val_aggregation)
    pass
