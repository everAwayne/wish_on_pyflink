import json
from kafka import KafkaProducer

wish_data = {
    "timestamp": 1613710790+86400*6,
    "pid": '57cd5eba980f802030b40028b',
    "merchant_id": 'zzzzz',
    "merchant_name": 'nixuanecommercecoltd',
    "shop_name": 'Ni Xuan E-Commerce Co., Ltd.',
    "review_number": 2248,
    "review_score": 4.46,
    "shop_review_number": 25257,
    "title": '300pcs/lot 11mm Hotfix Bling Acrylic Pointback Rhinestone Buttons Artificial Plastic Decorative',
    "is_pb": 0,
    "is_hwc": 0,
    "is_verified": 1,
    "total_bought": 52,
    "total_wishlist": 10000,
    "tags": ["Crystal", "button", "Rhinestone"],
    "category_ids": ["tag_53dc186421a86318bdc87f16", "tag_5488e99658f177327519fec4"],
    "category_paths": ["Accessories:Beads"],
    "category_l1_ids": ['tag_53dc186421a86318bdc87f16'],
    "category_l2_ids": ['tag_5488e99658f177327519fec4'],
    "category_l3_ids": [],
    "leaf_category_ids": ['tag_5488e99658f177327519fec4'],
    "price": 2.9,
    "shipping_price": 1.0,
    "sold": 0.93,
    "update_time": '2020-07-02 00:02:12',
    "shop_open_time": '2020-07-02 00:03:10',
    "gen_time": '2020-07-02 00:04:10',
    "data_update_time": '2020-07-02 00:05:10'
}

test_data = {
    "name": "abc",
    "value": 1,
    "count": 2
}

producer = KafkaProducer(bootstrap_servers='10.0.9.5:9092')
future = producer.send("wish-product-data", json.dumps(wish_data).encode("utf-8"))
#future = producer.send("test-data", json.dumps(test_data).encode("utf-8"))

result = future.get(timeout=60)
print(result)
#producer.flush()
