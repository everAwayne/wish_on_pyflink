import time
import random
import logging
import json
from itertools import accumulate
from datetime import datetime, timezone, timedelta

from typing_extensions import final
from base import FlinkJob
from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.table import DataTypes, ListView, MapView
from pyflink.table.expressions import col, Expression
from pyflink.table.udf import AggregateFunction, udaf

EFFICIENT_SECOND_LIMIT = 86400 * 14
TZ_SH = timezone(timedelta(hours=8), 'sh')
logger = logging.getLogger("test")


class CalculateAgg(AggregateFunction):

  def create_accumulator(self):
    return Row(False, 0, 0, ListView(), ListView(), ListView(), "")

  def get_value(self, accumulator):
    if not accumulator[0]:
      return
    sold_ls = list(accumulator[3])
    price_ls = list(accumulator[4])
    review_total_ls = list(accumulator[5])
    new_review_number_last_30 = review_total_ls[-1] - review_total_ls[-30]
    sold_last_1_pop = (sold_ls[-1] - sold_ls[-2]) / sold_ls[-2] \
                      if sold_ls[-2] else 0.0
    sold_last_1 = sum(sold_ls[-1:])
    sold_last_3 = sum(sold_ls[-3:])
    sold_last_7 = sum(sold_ls[-7:])
    sold_last_30 = sum(sold_ls[-30:])
    gmv_last_1 = sum(map(lambda x:x[0]*x[1], zip(sold_ls[-1:], price_ls[-1:])))
    gmv_last_3 = sum(map(lambda x:x[0]*x[1], zip(sold_ls[-3:], price_ls[-3:])))
    gmv_last_7 = sum(map(lambda x:x[0]*x[1], zip(sold_ls[-7:], price_ls[-7:])))
    gmv_last_30 = sum(map(lambda x:x[0]*x[1], zip(sold_ls[-30:], price_ls[-30:])))
    dct = {
            "new_review_number_last_30": new_review_number_last_30,
            "sold_last_1": round(sold_last_1, 2),
            "sold_last_3": round(sold_last_3, 2),
            "sold_last_7": round(sold_last_7, 2),
            "sold_last_30": round(sold_last_30, 2),
            "sold_last_1_pop": round(sold_last_1_pop, 6),
            "sold_2_to_last": round(sold_ls[-2], 2),
            "sold_3_to_last": round(sold_ls[-3], 2),
            "sold_4_to_last": round(sold_ls[-4], 2),
            "sold_5_to_last": round(sold_ls[-5], 2),
            "sold_6_to_last": round(sold_ls[-6], 2),
            "sold_7_to_last": round(sold_ls[-7], 2),
            "gmv_last_1": round(gmv_last_1, 2),
            "gmv_last_3": round(gmv_last_3, 2),
            "gmv_last_7": round(gmv_last_7, 2),
            "gmv_last_30": round(gmv_last_30, 2)}
    dct.update(json.loads(accumulator[6]))
    return json.dumps(dct)
            

  def accumulate(self, accumulator, timestamp, pid, merchant_id, merchant_name,
                 shop_name, review_number, review_score, shop_review_number,
                 title, is_pb, is_hwc, is_verified, total_bought,
                 total_wishlist, tags, category_ids, category_paths,
                 category_l1_ids, category_l2_ids, category_l3_ids,
                 leaf_category_ids, price, shipping_price, sold, update_time,
                 shop_open_time, gen_time, data_update_time, limit=30):
    try:
      batch_num = int(update_time.timestamp())
      if EFFICIENT_SECOND_LIMIT + timestamp < time.time() \
              or accumulator[2] == batch_num \
              or accumulator[1] > timestamp:
        accumulator[0] = False
        return
      # update
      idx_map = {
        3: sold,
        4: price,
        5: review_number
      }
      date_start = datetime.fromtimestamp(accumulator[1], TZ_SH).date()
      date_end = datetime.fromtimestamp(timestamp, TZ_SH).date()
      if date_start == date_end:
        for idx, new_data in idx_map.items():
          ls = list(accumulator[idx])
          ls[-1] = new_data
          accumulator[idx].clear()
          accumulator[idx].add_all(ls)
      elif accumulator[1] == 0:
        for idx, new_data in idx_map.items():
          accumulator[idx].add(new_data)
      else:
        for idx, new_data in idx_map.items():
          ls = list(accumulator[idx])
          accumulator[idx].add_all(self._generate_middle_data(date_start, date_end, ls[-1], new_data))
      # fill and truncate
      for idx, new_data in idx_map.items():
        change = False
        ls = list(accumulator[idx])
        if len(ls) < limit:
          ls = [new_data] * (limit - len(ls)) + ls
          change = True
        if len(ls) > limit:
          ls = ls[-1*limit:]
          change = True
        if change:
          accumulator[idx].clear()
          accumulator[idx].add_all(ls)
      accumulator[0] = True
      accumulator[1] = timestamp
      accumulator[2] = batch_num
      accumulator[6] = json.dumps({
        "timestamp": timestamp, "pid": pid, "merchant_id": merchant_id,
        "merchant_name": merchant_name, "shop_name": shop_name,
        "review_number": review_number, "review_score": round(review_score, 2),
        "shop_review_number": shop_review_number, "title": title,
        "is_pb": is_pb, "is_hwc":is_hwc, "is_verified":is_verified,
        "total_bought":total_bought, "total_wishlist": total_wishlist,
        "tags": tags, "category_ids": category_ids,
        "category_paths": category_paths, "category_l1_ids":category_l1_ids,
        "category_l2_ids":category_l2_ids, "category_l3_ids": category_l3_ids,
        "leaf_category_ids": leaf_category_ids, "price": round(price, 2),
        "shipping_price": shipping_price, "sold": round(sold, 2),
        "update_time": update_time.strftime("%Y-%m-%d %H:%M:%S"),
        "shop_open_time": shop_open_time.strftime("%Y-%m-%d %H:%M:%S"),
        "gen_time": gen_time.strftime("%Y-%m-%d %H:%M:%S"),
        "data_update_time": data_update_time.strftime("%Y-%m-%d %H:%M:%S")
      })
    except Exception as exc:
        exc_info = (type(exc), exc, exc.__traceback__)
        logger.error('[ERROR]', exc_info=exc_info)
  
  def _generate_middle_data(self, date_start, date_end, data1, data2):
    days = (date_end - date_start).days
    diff = data2 - data1
    if isinstance(data1, float):
      deltas = [round(diff / days, 2)] * (days - 1)
      deltas.append(round(diff-sum(deltas), 2))
    elif isinstance(data1, int):
      deltas = [diff // days] * days
      for i in random.sample(range(days), diff % days):
        deltas[i] += 1
    deltas[0] += data1
    return accumulate(deltas)

  def get_accumulator_type(self):
    return DataTypes.ROW([
        DataTypes.FIELD("available", DataTypes.BOOLEAN()),
        DataTypes.FIELD("timestamp", DataTypes.BIGINT()),
        DataTypes.FIELD("batch_num", DataTypes.BIGINT()),
        DataTypes.FIELD("sold_last", DataTypes.LIST_VIEW(DataTypes.FLOAT())),
        DataTypes.FIELD("price_last", DataTypes.LIST_VIEW(DataTypes.FLOAT())),
        DataTypes.FIELD("review_total_last", DataTypes.LIST_VIEW(DataTypes.INT())),
        DataTypes.FIELD("info", DataTypes.STRING())])

  def get_result_type(self):
    return DataTypes.STRING()


class ShopAgg(AggregateFunction):

  def create_accumulator(self):
    return Row("", 0)
  
  def get_value(self, accumulator):
    return json.dumps({"date": accumulator[0], "count": accumulator[1]})
  
  def accumulate(self, accumulator, date, count):
    date = str(date.date())
    if accumulator[0] == date:
      accumulator[1] += count
      return
    accumulator[0] = date
    accumulator[1] = count

  def get_accumulator_type(self):
    return DataTypes.ROW([
        DataTypes.FIELD("date", DataTypes.STRING()),
        DataTypes.FIELD("count", DataTypes.INT())])

  def get_result_type(self):
    return DataTypes.STRING()


class ProductJob(FlinkJob):

    job_name = "wish:product"
    kafka_addr = "10.0.9.5:9092"
    wish_data_topic = "wish-product-data"
    wish_result_topic = "wish-product-result-data"
    wish_shop_result_topic = "wish-shop-result-data"
    tables = [
      f"""
        CREATE TABLE wish_product_data (
          `timestamp` INT,
          `pid` STRING,
          `merchant_id` STRING,
          `merchant_name` STRING,
          `shop_name` STRING,
          `review_number` INT,
          `review_score` FLOAT,
          `shop_review_number` INT,
          `title` STRING,
          `is_pb` BOOLEAN,
          `is_hwc` BOOLEAN,
          `is_verified` BOOLEAN,
          `total_bought` INT,
          `total_wishlist` INT,
          `tags` ARRAY<STRING>,
          `category_ids` ARRAY<STRING>,
          `category_paths` ARRAY<STRING>,
          `category_l1_ids` ARRAY<STRING>,
          `category_l2_ids` ARRAY<STRING>,
          `category_l3_ids` ARRAY<STRING>,
          `leaf_category_ids` ARRAY<STRING>,
          `price` FLOAT,
          `shipping_price` FLOAT,
          `sold` FLOAT,
          `update_time` TIMESTAMP(0),
          `shop_open_time` TIMESTAMP(0),
          `gen_time` TIMESTAMP(0),
          `data_update_time` TIMESTAMP(0)
        ) WITH (
          'connector' = 'kafka',
          'topic' = '{wish_data_topic}',
          'properties.bootstrap.servers' = '{kafka_addr}',
          'properties.group.id' = 'flink_bigdata',
          'scan.startup.mode' = 'group-offsets',
          'format' = 'json'
        )
      """
    ]
    result_type = Types.ROW_NAMED(
      ["infos"],
      [Types.STRING()]
    )
    shop_result_type = Types.ROW_NAMED(
      ["shop_name", "shop_agg"],
      [Types.STRING(), Types.STRING()]
    )

    def process(self) -> None:
      calculate = udaf(CalculateAgg())
      shop_agg = udaf(ShopAgg())
      table = self.table_env.from_path("wish_product_data")
      result_table = table.group_by(table.pid)\
                    .select(calculate(table.timestamp, table.pid, table.merchant_id,
                                      table.merchant_name, table.shop_name, table.review_number,
                                      table.review_score, table.shop_review_number,
                                      table.title, table.is_pb, table.is_hwc, table.is_verified,
                                      table.total_bought, table.total_wishlist, table.tags,
                                      table.category_ids, table.category_paths,
                                      table.category_l1_ids, table.category_l2_ids,
                                      table.category_l3_ids, table.leaf_category_ids,
                                      table.price, table.shipping_price, table.sold, table.update_time,
                                      table.shop_open_time, table.gen_time, table.data_update_time))
                            #calculate(table.timestamp, table.update_time, table.sold, table.price, table.review_number))
      shop_table = table.select(table.update_time, table.shop_name)
      shop_table = shop_table.add_columns("1 as count")
      shop_result_table = shop_table.group_by(shop_table.shop_name)\
          .select(shop_table.shop_name, shop_agg(table.update_time, shop_table.count))
      self.sink_to_kafka(self.wish_result_topic, result_table, self.result_type)
      self.sink_to_kafka(self.wish_shop_result_topic, shop_result_table, self.shop_result_type)
      #stmt_set = self.table_env.create_statement_set()
      #stmt_set.add_insert("print_sink", result_table)
      #stmt_set.add_insert("print_shop_sink", shop_result_table)
      #stmt_set.execute().wait()
    
    #def run(self) -> None:
    #  self.process()


if __name__ == "__main__":
    job = ProductJob(parallelism=2, checkpoint_interval=30)
    job.run()
