import datetime
import io
import json
import logging
import os
import re
import tempfile

import botocore
import boto3

import matplotlib.dates
import matplotlib.pyplot as plt
import matplotlib.style as mplstyle
import matplotlib.ticker

import numpy as np

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DESTINATION_BUCKET = os.environ.get('GRAPH_DESTINATION_BUCKET', None)
PUBLISH_TO_SLACK_ARN = os.environ.get('PUBLISH_TO_SLACK_ARN', None)

mplstyle.use('fast')


class WeekdayDateFormatter(matplotlib.ticker.Formatter):
    def __init__(self, dates, fmt='%Y-%m-%d'):
        self.dates = list(map(matplotlib.dates.num2date, dates))
        self.fmt = fmt

    def __call__(self, x, pos=0):
        ind = int(np.round(x))
        if ind >= len(self.dates) or ind < 0:
            return ''

        return self.dates[ind].strftime(self.fmt)


class DateParser(object):
    def __init__(self):
        self.dates_only = False

    def parse(self, d):
        if self.dates_only:
            return datetime.datetime.strptime(d, '%Y-%m-%d')
        else:
            try:
                return datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S')
            except ValueError as e:
                logger.warning("Error parsing date '%s', trying again with date only (no time)", d)
                self.dates_only = True
                return self.parse(d)


def generate_graph(graph: dict):
    logger.info("X:{}, Y:{}".format(graph['xaxis'], graph['yaxis']))

    xlabel = graph.get('xlabel', "x")
    ylabel = graph.get('ylabel', "y")
    title = graph.get('title', "My Graph")

    fig, ax = plt.subplots(figsize=(5, 5))

    try:
        dp = DateParser()
        xaxis = [
            matplotlib.dates.date2num(dp.parse(d))
            for d in graph['xaxis']
        ]

        logger.debug("X-Axis: (%d points) %s", len(xaxis), str(xaxis))

        x_axis_fmt = '%-m/%-d %H:%M'
        if dp.dates_only:
            x_axis_fmt = '%-m/%-d'

        formatter = WeekdayDateFormatter(xaxis, fmt=x_axis_fmt)
        locator = matplotlib.ticker.IndexLocator(
            base=len(xaxis) // 5 - 1,
            offset=0
        )

        ax.xaxis.set_major_formatter(formatter)
        ax.xaxis.set_major_locator(locator)

        ax.plot(np.arange(len(xaxis)), graph['yaxis'])
        fig.autofmt_xdate()
    except ValueError as e:
        logger.exception("Error formatting dates")
        ax.plot(graph['xaxis'], graph['yaxis'])
        ax.xaxis.set_major_locator(plt.MaxNLocator(5))

    ax.set(xlabel=xlabel, ylabel=ylabel, title=title)
    ax.yaxis.set_major_locator(plt.MaxNLocator(5))

    fig.tight_layout()

    return fig


def validate_data(data: dict):
    if 'graph' not in data:
        raise ValueError("Missing 'graph' field")

    graph = data['graph']
    if 'xaxis' not in graph:
        raise ValueError('Missing "xaxis" in data')

    if 'yaxis' not in graph:
        raise ValueError('Missing "yaxis" in data')

    if len(graph['xaxis']) != len(graph['yaxis']):
        raise ValueError("xaxis and yaxis must have the same dimension")


def get_url(bucket_name, key_name):
    return 'https://s3.amazonaws.com/{}/{}'.format(
        bucket_name, key_name
    )


def lambda_handler(data: dict, event):
    logger.info("Invoked with data: %s", json.dumps(data))
    validate_data(data)

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(DESTINATION_BUCKET)
    key_name = re.sub(r"\s+", '-', "{}-{}.png".format(
                        data['symbol'], data['date']))

    objs = list(bucket.objects.filter(Prefix=key_name))
    if objs and objs[0].key == key_name:
        url = get_url(bucket.name, key_name)
        logger.info("Found existing file, no need to generate a new graph: %s", url)
    else:
        fig = generate_graph(data['graph'])

        logger.info("Saving image to PNG format")

        url = None
        try:
            with tempfile.TemporaryFile() as img_file:
                fig.savefig(img_file, format='png')
                img_file.flush()
                img_file.seek(0)

                logger.info("Uploading to s3 bucket:%s key:%s", bucket.name,
                            key_name)
                bucket.put_object(
                    Body=img_file,
                    ACL='public-read',
                    ContentType='image/png',
                    Key=key_name
                )

            url = get_url(bucket.name, key_name)
            logger.info("Saved to url: %s", url)
        except botocore.exceptions.ClientError as e:
            logger.exception("Error uploading to S3")
            raise

    if 'destination' in data:
        destination = data['destination']
        if 'slack_channel' in destination:
            slack_message_payload = {
                'channel': destination['slack_channel'],
                'text': data['message_text'],
                'attachments': [
                    {
                        'fallback': data['message_text'],
                        'image_url': url
                    }
                ]
            }
            payload = io.BytesIO(json.dumps(slack_message_payload, ensure_ascii=False).encode('utf-8'))
            lambda_client = boto3.client('lambda')

            res = lambda_client.invoke(
                FunctionName=PUBLISH_TO_SLACK_ARN,
                InvocationType='Event',
                Payload=payload
            )

            logger.info("Invoked lambda: %s", res)

    return url


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--log-level',
                        choices=('DEBUG', 'INFO', 'WARN', 'ERROR', 'TRACE'),
                        default='DEBUG')
    parser.add_argument('-o', '--output-file', help='Output location', type=str,
                        default='out.png')
    parser.add_argument('input_file', help='JSON formatted data')

    options = parser.parse_args()
    logging.basicConfig(level=getattr(logging, options.log_level))
    try:
        with open(options.input_file, 'r') as input_file:
            data = json.load(input_file)
            fig = generate_graph(data['graph'])
    except:
        logger.exception("Failed to generate graph")
    else:
        with open(options.output_file, 'wb') as output_file:
            fig.savefig(output_file, format='png')
