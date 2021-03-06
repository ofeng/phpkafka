/**
 *  Copyright 2013-2014 Patrick Reilly.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
#include <php.h>
#include "kafka.h"
#include <php_kafka.h>

#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <syslog.h>
#include <sys/time.h>
#include <errno.h>
#include <syslog.h>
#include <time.h>
#include "kafka.h"
#include "librdkafka/rdkafka.h"

int64_t start_offset = 0;

static struct conf {
        int     run;
        int     verbosity;
        int     exitcode;
        char    mode;
        int     delim;
        int     msg_size;
        char   *brokers;
        char   *topic;
        int32_t partition;
        int64_t offset;
        int     exit_eof;
        int64_t msg_cnt;

        rd_kafka_conf_t       *rk_conf;
        rd_kafka_topic_conf_t *rkt_conf;

        rd_kafka_t            *rk;
        rd_kafka_topic_t      *rkt;

        char   *debug;
        int     conf_dump;
} conf = {
        .run = 1,
        .verbosity = 1,
        .partition = RD_KAFKA_PARTITION_UA,
        .msg_size = 1024*1024,
        .delim = '\n',
        .exit_eof = 1,
};

void kafka_connect(char *brokers)
{
    conf.brokers = brokers;
}

void kafka_set_partition(int partition)
{
    conf.partition = partition;
}

void kafka_set_topic(char *topic)
{
    php_printf("%s\n", topic);
    conf.topic = topic;
}

void kafka_stop(int sig) {
    conf.run = 0;
    rd_kafka_destroy(conf.rk);
    conf.rk = NULL;
}

void kafka_err_cb (rd_kafka_t *rk, int err, const char *reason, void *opaque) {
    openlog("phpkafka", 0, LOG_USER);
    syslog(LOG_INFO, "phpkafka - ERROR CALLBACK: %s: %s: %s\n",
            rd_kafka_name(rk), rd_kafka_err2str(err), reason);

    kafka_stop(err);
}

void kafka_msg_delivered (rd_kafka_t *rk,
                           void *payload, size_t len,
                           int error_code,
                           void *opaque, void *msg_opaque) {
    if (error_code) {
        openlog("phpkafka", 0, LOG_USER);
        syslog(LOG_INFO, "phpkafka - Message delivery failed: %s",
                rd_kafka_err2str(error_code));
    }
}

void kafka_destroy()
{
    if(conf.rk != NULL) {
        /* Wait for all messages to be transmitted */
        while (conf.run && rd_kafka_outq_len(conf.rk) > 0)
          rd_kafka_poll(conf.rk, 50);
        rd_kafka_destroy(conf.rk);
        rd_kafka_wait_destroyed(1000);
        conf.rk = NULL;
    }
}

void producer_setup()
{
    char errstr[512];
    /* Create config containers */
    conf.rk_conf  = rd_kafka_conf_new();

    /* Create producer */
    if (!(conf.rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf.rk_conf,
                                 errstr, sizeof(errstr)))) {
            openlog("phpkafka", 0, LOG_USER);
            syslog(LOG_INFO, "phpkafka - failed to create new producer: %s", errstr);
            exit(1);
    }

    /* Add brokers */
    if (rd_kafka_brokers_add(conf.rk, conf.brokers) == 0) {
            openlog("phpkafka", 0, LOG_USER);
            syslog(LOG_INFO, "php kafka - No valid brokers specified");
            exit(1);
    }

    /* Set up a message delivery report callback.
     * It will be called once for each message, either on successful
     * delivery to broker, or upon failure to deliver to broker. */
    //rd_kafka_conf_set_dr_cb(conf.rk_conf, kafka_msg_delivered);
    //rd_kafka_conf_set_error_cb(conf.rk_conf, kafka_err_cb);

    // openlog("phpkafka", 0, LOG_USER);
    // syslog(LOG_INFO, "phpkafka - using: %s", brokers);

    /* Topic configuration */
    conf.rkt_conf = rd_kafka_topic_conf_new();

    /* Create topic */
    if (!(conf.rkt = rd_kafka_topic_new(conf.rk, conf.topic,
                                            conf.rkt_conf))) {
      openlog("phpkafka", 0, LOG_USER);
      syslog(LOG_INFO, "phpkafka - failed to create topic: %s: %s", conf.topic,
                      rd_kafka_err2str(rd_kafka_errno2err(errno)));
      exit(1);
    }

    conf.rk_conf  = NULL;
    conf.rkt_conf = NULL;
}

void kafka_produce(char* msg, size_t msg_len)
{
    static int initialized = 0;
    if (!initialized){
      producer_setup();
      php_printf("%s\n", "*** CREATED NEW PRODUCER! ***");
      initialized = 1;
    }

    // signal(SIGINT, kafka_stop);
    // signal(SIGTERM, kafka_stop);
    // signal(SIGPIPE, kafka_stop);

      if (rd_kafka_produce(conf.rkt, conf.partition,
                       RD_KAFKA_MSG_F_COPY,
                       msg, msg_len, NULL, 0, NULL) == -1) {
          openlog("phpkafka", 0, LOG_USER);
          syslog(LOG_INFO, "phpkafka - %% Failed to produce to topic %s "
          "partition %i: %s",
          rd_kafka_topic_name(conf.rkt), conf.partition,
          rd_kafka_err2str(rd_kafka_errno2err(errno)));
        rd_kafka_poll(conf.rk, 0);
      }

    /* Poll to handle delivery reports */
    rd_kafka_poll(conf.rk, 0);
}

static rd_kafka_message_t *msg_consume(rd_kafka_message_t *rkmessage, void *opaque) {
  if (rkmessage->err) {
    if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
      openlog("phpkafka", 0, LOG_USER);
      syslog(LOG_INFO,
        "phpkafka - %% Consumer reached end of %s [%"PRId32"] "
             "message queue at offset %"PRId64"\n",
             rd_kafka_topic_name(rkmessage->rkt),
             rkmessage->partition, rkmessage->offset);
      if (conf.exit_eof)
        conf.run = 0;
      return NULL;
    }

    openlog("phpkafka", 0, LOG_USER);
    syslog(LOG_INFO, "phpkafka - %% Consume error for topic \"%s\" [%"PRId32"] "
           "offset %"PRId64": %s\n",
           rd_kafka_topic_name(rkmessage->rkt),
           rkmessage->partition,
           rkmessage->offset,
           rd_kafka_message_errstr(rkmessage));
    return NULL;
  }

  return rkmessage;
}

void kafka_consume(zval* return_value, char* offset, int item_count)
{

  int read_counter = 0;

  if (strlen(offset) != 0) {
    if (!strcmp(offset, "end"))
      start_offset = RD_KAFKA_OFFSET_END;
    else if (!strcmp(offset, "beginning"))
      start_offset = RD_KAFKA_OFFSET_BEGINNING;
    else if (!strcmp(offset, "stored"))
      start_offset = RD_KAFKA_OFFSET_STORED;
    else
      start_offset = strtoll(offset, NULL, 10);
  }

    char errstr[512];

    /* Create Kafka handle */
    if (!(conf.rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf.rk_conf,
                                 errstr, sizeof(errstr)))) {
                  openlog("phpkafka", 0, LOG_USER);
                  syslog(LOG_INFO, "phpkafka - failed to create new consumer: %s", errstr);
                  exit(1);
    }

    /* Add brokers */
    if (rd_kafka_brokers_add(conf.rk, conf.brokers) == 0) {
            openlog("phpkafka", 0, LOG_USER);
            syslog(LOG_INFO, "php kafka - No valid brokers specified");
            exit(1);
    }

    /* Create topic */
    if (!(conf.rkt = rd_kafka_topic_new(conf.rk, conf.topic, conf.rkt_conf)));

    openlog("phpkafka", 0, LOG_USER);
    syslog(LOG_INFO, "phpkafka - start_offset: %d and offset passed: %d", start_offset, offset);

    /* Start consuming */
    if (rd_kafka_consume_start(conf.rkt, conf.partition, start_offset) == -1) {
      openlog("phpkafka", 0, LOG_USER);
      syslog(LOG_INFO, "phpkafka - %% Failed to start consuming: %s",
        rd_kafka_err2str(rd_kafka_errno2err(errno)));
      exit(1);
    }

    if (item_count != 0) {
      read_counter = item_count;
    }

    while (conf.run) {
      if (item_count != 0 && read_counter >= 0) {
        read_counter--;
        openlog("phpkafka", 0, LOG_USER);
        syslog(LOG_INFO, "phpkafka - read_counter: %d", read_counter);
        if (read_counter == -1) {
          conf.run = 0;
          continue;
        }
      }

      rd_kafka_message_t *rkmessage;

      /* Consume single message.
       * See rdkafka_performance.c for high speed
       * consuming of messages. */
      rkmessage = rd_kafka_consume(conf.rkt, conf.partition, 1000);
      if (!rkmessage) /* timeout */
        continue;

      rd_kafka_message_t *rkmessage_return;
      rkmessage_return = msg_consume(rkmessage, NULL);
      char payload[(int)rkmessage_return->len];
      sprintf(payload, "%.*s", (int)rkmessage_return->len, (char *)rkmessage_return->payload);
      add_index_string(return_value, (int)rkmessage_return->offset, payload, 1);

      /* Return message to rdkafka */
      rd_kafka_message_destroy(rkmessage);
    }

    /* Stop consuming */
    rd_kafka_consume_stop(conf.rkt, conf.partition);
    rd_kafka_topic_destroy(conf.rkt);
    rd_kafka_destroy(conf.rk);
}
