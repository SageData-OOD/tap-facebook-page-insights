#!/usr/bin/env python3
import os
import json
import facebook
import singer
from datetime import datetime, timedelta, date
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from singer.transform import transform

REQUIRED_CONFIG_KEYS = ["start_date", "access_token", "fb_object_ids"]
LOGGER = singer.get_logger()


def get_key_properties(stream_id):
    key_properties = {
        "page_insights": ["created_ts", "page_id", "value_name", "title"],
        "feed_insights": ["created_ts", "post_created_time", "page_id", "post_id"]
    }

    return key_properties.get(stream_id, [])


def get_bookmark(stream_id):
    """
    Bookmarks for the streams which has incremental sync.
    """
    bookmarks = {
        "page_insights": "created_ts",
        "feed_insights": "created_ts"
    }
    return bookmarks.get(stream_id)


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def create_metadata_for_report(stream_id, schema, key_properties):
    replication_key = get_bookmark(stream_id)
    mdata = [{"breadcrumb": [], "metadata": {"inclusion": "available", "forced-replication-method": "INCREMENTAL",
                                             "valid-replication-keys": [replication_key], "selected": True}}]

    if key_properties:
        mdata[0]["metadata"]["table-key-properties"] = key_properties

    if not replication_key:
        mdata[0]["metadata"]["forced-replication-method"] = "FULL_TABLE"
        mdata[0]["metadata"].pop("valid-replication-keys")

    for key in schema.properties:
        # hence, when property is object, we will only consider properties of that object without taking object itself.
        if "object" in schema.properties.get(key).type and schema.properties.get(key).properties:
            inclusion = "available"
            mdata.extend(
                [{"breadcrumb": ["properties", key, "properties", prop],
                  "metadata": {"inclusion": inclusion, "selected": True}} for prop
                 in schema.properties.get(key).properties])
        else:
            inclusion = "automatic" if key in key_properties + [replication_key] else "available"
            mdata.append({"breadcrumb": ["properties", key], "metadata": {"inclusion": inclusion, "selected": True}})

    return mdata


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = create_metadata_for_report(stream_id, schema, get_key_properties(stream_id))
        key_properties = get_key_properties(stream_id)
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata
            )
        )
    return Catalog(streams)


def date_today():
    return datetime.today().date()


def string_to_date(_date):
    return datetime.strptime(_date, "%Y-%m-%dT%H:%M:%S.%fZ").date()


def get_verified_date_to_poll(date_to_poll):
    date_to_poll = datetime.strptime(date_to_poll, "%Y-%m-%d")
    return date_to_poll.date()


def get_end_date(start_date):
    # if end_date bigger than today's date, then end_date = today
    end_date = min(start_date + timedelta(days=30), date.today())

    return end_date


# Query Data
def query_page_name(fb_graph, business_account_id):
    page_info = fb_graph.get_object(id=business_account_id, fields="name")
    page_name = page_info["name"]
    return page_name


def query_page_insights(fb_graph, business_account_id, page_name, metrics, start_date, end_date):
    all_insights = []

    connection_name = "&".join(
        ["insights?period=day&since={}&until={}".format(start_date, end_date),
         "metric={}".format(",".join(metrics))]
    )
    LOGGER.info("connection_name: %s", connection_name)
    page_insights_generator = fb_graph.get_all_connections(id=business_account_id,
                                                           connection_name=connection_name)

    field_names = ("created_ts", "page_id", "page_name", "value_name", "title", "value")
    for insight in page_insights_generator:
        for value in insight['values']:
            if value['end_time'] >= str(end_date):
                continue
            if isinstance(value['value'], dict):
                for title in value['value']:
                    all_insights.append(dict(zip(field_names,
                                                 [value['end_time'],
                                                  business_account_id,
                                                  page_name,
                                                  insight['name'],
                                                  title,
                                                  value['value'].get(title)])))
            else:
                title = 'total'
                all_insights.append(dict(zip(field_names,
                                             [value['end_time'],
                                              business_account_id,
                                              page_name,
                                              insight['name'],
                                              title,
                                              value['value']])))
    return all_insights


def query_posts(fb_graph, business_account_id, page_name, start_date, end_date):
    posts = {}
    connection_name = "feed?since={}&until={}".format(start_date, end_date)
    LOGGER.info("Query connection name: %s", connection_name)

    # this reads all feeds
    posts_generator = fb_graph.get_all_connections(
        id=business_account_id, connection_name=connection_name)

    for post in posts_generator:
        LOGGER.info("type: %s, id: %s", connection_name, post.get("id"))

        feed_message = post.get('message', post.get("story", ''))
        posts[post['id']] = {
            "created_ts": post["created_time"],
            "page_id": business_account_id,
            "page_name": page_name,
            "hashtags_count": 0,
            "post_id": post['id'],
            "message": feed_message
        }

        for word in feed_message.split():
            if word[0] == "#":
                posts[post['id']]["hashtags_count"] += 1

    LOGGER.info("Total %s queried: %d", connection_name, len(posts))

    return posts


def query_feeds_insights(fb_graph, business_account_id, start_date, end_date, page_name):
    all_posts = query_posts(fb_graph, business_account_id, page_name,
                            start_date=start_date,
                            end_date=end_date)

    return query_post_metrics(fb_graph, all_posts)


def query_post_metrics(fb_graph, posts):
    if not posts:
        return []

    fields = ["message_tags"]
    metrics = ["comments", "shares", "reactions"]
    for metric in metrics:
        fields.append("{}.limit(0).summary(1)".format(metric))
    reaction_types = ["like", "love", "wow", "haha", "sad", "angry"]
    for reaction in reaction_types:
        fields.append("reactions.type({}).limit(0).summary(1).as({})".format(reaction.upper(), reaction))

    post_ids = list(posts.keys())
    # 50 ids is max allowed by fb
    for i in range(0, len(posts), 50):
        post_ids_slice = post_ids[i:min(i + 50, len(post_ids))]
        reactions = fb_graph.get_objects(ids=post_ids_slice, fields=",".join(fields))

        for post_id in post_ids_slice:
            post = posts[post_id]
            fields_for_post = reactions[post_id]

            # reactions have a total_count summary field
            for field in reaction_types + metrics:
                # default count e.g. shares = 0
                post[field] = 0

                if not field in fields_for_post:
                    continue

                if "summary" in fields_for_post[field]:
                    post[field] = fields_for_post[field]["summary"]["total_count"]
                else:
                    post[field] = fields_for_post[field]["count"]

            # insights
            if "insights" in fields_for_post:
                for insight in fields_for_post["insights"]["data"]:
                    post[insight["name"]] = insight["values"][0]["value"]

            # tags
            post["tags"] = []
            if "message_tags" in fields_for_post:
                tagged_ids = set()
                for tag in fields_for_post["message_tags"]:
                    if tag["id"] in tagged_ids:
                        continue
                    tagged_ids.add(tag["id"])
                    post["tags"].append({
                        "tagged_id": tag["id"],
                        "tagged_name": tag["name"]
                    })

    return list(posts.values())


# Syncs
def sync_page_insights(user_graph, config, state, stream):
    mdata = metadata.to_map(stream.metadata)
    schema = stream.schema.to_dict()

    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=schema,
        key_properties=stream.key_properties,
    )
    metrics = [
        'page_impressions',
        'page_impressions_unique',
        'page_impressions_by_country_unique',
        'page_impressions_by_age_gender_unique',
        'page_impressions_viral',
        'page_impressions_viral_unique',
        'page_engaged_users',
        'page_post_engagements',
        'page_consumptions_unique',
        'page_consumptions_by_consumption_type',
        'page_fans_online',
        'page_fans_online_per_day',
        'page_positive_feedback_by_type',
        'page_positive_feedback_by_type_unique',
        'page_views_total',
        'page_views_by_age_gender_logged_in_unique',
        'page_video_views',
        'page_video_views_unique',
        'page_fans_gender_age',
        'page_fans_country',
        'page_fans',
        'page_fan_adds',
        'page_fan_adds_unique',
        'page_fans_by_like_source',
        'page_fans_by_like_source_unique'
    ]
    bookmark_column = get_bookmark(stream.tap_stream_id)
    start_date = singer.get_bookmark(state, stream.tap_stream_id, bookmark_column) \
        if state.get("bookmarks", {}).get(stream.tap_stream_id) \
        else config["start_date"]

    start_date = get_verified_date_to_poll(start_date, )
    global_bookmark = start_date
    for business_account_id in config["fb_object_ids"]:
        page_access_token = user_graph.get_object(id=business_account_id, fields="access_token")
        fb_graph = facebook.GraphAPI(access_token=page_access_token["access_token"], version=config["fb_api_version"])
        page_name = query_page_name(fb_graph, business_account_id)

        local_bookmark = start_date
        while True:
            end_date = get_end_date(local_bookmark)
            records = query_page_insights(fb_graph, business_account_id,
                                          page_name, metrics,
                                          start_date=local_bookmark,
                                          end_date=end_date)
            with singer.metrics.record_counter(stream.tap_stream_id) as counter:
                for row in records:
                    # Type Conversation and Transformation
                    transformed_data = transform(row, schema, metadata=mdata)

                    singer.write_records(stream.tap_stream_id, [transformed_data])
                    counter.increment()
                    if bookmark_column:
                        local_bookmark = max([local_bookmark, string_to_date(transformed_data[bookmark_column])])
                if end_date >= date_today():
                    break
                local_bookmark = end_date
        global_bookmark = max([local_bookmark, global_bookmark])

    if bookmark_column:
        state = singer.write_bookmark(state, stream.tap_stream_id, bookmark_column, str(global_bookmark))
        singer.write_state(state)


def sync_feed_insights(user_graph, config, state, stream):
    mdata = metadata.to_map(stream.metadata)
    schema = stream.schema.to_dict()

    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=schema,
        key_properties=stream.key_properties,
    )

    bookmark_column = get_bookmark(stream.tap_stream_id)
    start_date = singer.get_bookmark(state, stream.tap_stream_id, bookmark_column) \
        if state.get("bookmarks", {}).get(stream.tap_stream_id) \
        else config["start_date"]

    start_date = get_verified_date_to_poll(start_date)
    global_bookmark = start_date
    for business_account_id in config["fb_object_ids"]:
        page_access_token = user_graph.get_object(id=business_account_id, fields="access_token")
        fb_graph = facebook.GraphAPI(access_token=page_access_token["access_token"], version=config["fb_api_version"])
        page_name = query_page_name(fb_graph, business_account_id)

        local_bookmark = start_date

        while True:
            end_date = get_end_date(local_bookmark)
            records = query_feeds_insights(fb_graph, business_account_id,
                                           start_date=local_bookmark,
                                           end_date=end_date,
                                           page_name=page_name)
            with singer.metrics.record_counter(stream.tap_stream_id) as counter:
                for row in records:
                    # Type Conversation and Transformation
                    transformed_data = transform(row, schema, metadata=mdata)

                    singer.write_records(stream.tap_stream_id, [transformed_data])
                    counter.increment()
                    if bookmark_column:
                        local_bookmark = max([local_bookmark, string_to_date(transformed_data[bookmark_column])])
                if end_date >= date_today():
                    break
                local_bookmark = end_date
        global_bookmark = max([local_bookmark, global_bookmark])

    if bookmark_column:
        state = singer.write_bookmark(state, stream.tap_stream_id, bookmark_column, str(global_bookmark))
        singer.write_state(state)


def sync(config, state, catalog):
    selected_streams = catalog.get_selected_streams(state)
    if not selected_streams:
        return

    config["fb_api_version"] = config.get("fb_api_version", 3.1)
    user_graph = facebook.GraphAPI(access_token=config["access_token"], version=config["fb_api_version"])

    # Loop over selected streams in catalog
    for stream in selected_streams:
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)
        if stream.tap_stream_id in ["page_insights"]:
            sync_page_insights(user_graph, config, state, stream)
        elif stream.tap_stream_id in ["feed_insights"]:
            sync_feed_insights(user_graph, config, state, stream)


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        catalog = discover()
        catalog.dump()
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
