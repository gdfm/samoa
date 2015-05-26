package com.yahoo.labs.samoa.topology.impl;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2013 Yahoo! Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.core.EntranceProcessor;
import com.yahoo.labs.samoa.topology.AbstractEntranceProcessingItem;
import com.yahoo.labs.samoa.topology.EntranceProcessingItem;
import com.yahoo.labs.samoa.topology.Stream;

/**
 * EntranceProcessingItem implementation for Storm.
 */
class StormEntranceProcessingItem extends AbstractEntranceProcessingItem implements StormTopologyNode {
    private static final Logger LOG = LoggerFactory.getLogger(StormEntranceProcessingItem.class);

    private final StormEntranceSpout piSpout;

    StormEntranceProcessingItem(EntranceProcessor processor) {
        this(processor, UUID.randomUUID().toString());
    }

    StormEntranceProcessingItem(EntranceProcessor processor, String friendlyId) {
    	super(processor);
    	this.setName(friendlyId);
        this.piSpout = new StormEntranceSpout(processor);
    }

    @Override
    public EntranceProcessingItem setOutputStream(Stream stream) {
        // piSpout.streams.add(stream);
        piSpout.setOutputStream((StormStream) stream);
        return this;
    }
    
    @Override
    public Stream getOutputStream() {
    	return piSpout.getOutputStream();
    }
    
    @Override
    public void addToTopology(StormTopology topology, int parallelismHint) {
        topology.getStormBuilder().setSpout(this.getName(), piSpout, parallelismHint);
    }

    @Override
    public StormStream createStream() {
        return piSpout.createStream(this.getName());
    }

    @Override
    public String getId() {
        return this.getName();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.insert(0, String.format("id: %s, ", this.getName()));
        return sb.toString();
    }

    /**
     * Resulting Spout of StormEntranceProcessingItem
     */
    final static class StormEntranceSpout extends BaseRichSpout {

        private static final long serialVersionUID = -9066409791668954099L;
        private static final Object anchor = new Object();

        // private final Set<StormSpoutStream> streams;
        private final EntranceProcessor entranceProcessor;
        private StormStream outputStream;
        private int acked, failed;

        // private transient SpoutStarter spoutStarter;
        // private transient Executor spoutExecutors;
        // private transient LinkedBlockingQueue<StormTupleInfo> tupleInfoQueue;

        private SpoutOutputCollector collector;

        StormEntranceSpout(EntranceProcessor processor) {
            // this.streams = new HashSet<StormSpoutStream>();
            this.entranceProcessor = processor;
        }

        public StormStream getOutputStream() {
            return outputStream;
        }

        public void setOutputStream(StormStream stream) {
            this.outputStream = stream;
        }

        @Override
        public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
            // this.tupleInfoQueue = new LinkedBlockingQueue<StormTupleInfo>();

            // Processor and this class share the same instance of stream
            // for (StormSpoutStream stream : streams) {
            // stream.setSpout(this);
            // }
            // outputStream.setSpout(this);

            this.entranceProcessor.onCreate(context.getThisTaskId());
            // this.spoutStarter = new SpoutStarter(this.starter);

            // this.spoutExecutors = Executors.newSingleThreadExecutor();
            // this.spoutExecutors.execute(spoutStarter);
        }

        @Override
        public void nextTuple() {
            if (entranceProcessor.hasNext()) {
                Values value = newValues(entranceProcessor.nextEvent());
                collector.emit(outputStream.getOutputId(), value, anchor);
            } else
                Utils.sleep(1000);
            // StormTupleInfo tupleInfo = tupleInfoQueue.poll(50, TimeUnit.MILLISECONDS);
            // if (tupleInfo != null) {
            // Values value = newValues(tupleInfo.getContentEvent());
            // collector.emit(tupleInfo.getStormStream().getOutputId(), value);
            // }
        }

        @Override
        public void activate() {
          super.activate();
          final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
          Runnable helloRunnable = new Runnable() {
            public void run() {
              report();
            }
          };
          executorService.scheduleAtFixedRate(helloRunnable, 0, 3, TimeUnit.SECONDS);
        }

        @Override
        public void ack(Object msgId) {
          super.ack(msgId);
          acked++;
        }

        @Override
        public void fail(Object msgId) {
          super.fail(msgId);
          failed++;
        }
        
        public void report() {
          LOG.info("ACKED {} \t FAILED {}", acked, failed);
        }

        @Override
        public void close() {
          super.close();
          report();
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // for (StormStream stream : streams) {
            // declarer.declareStream(stream.getOutputId(), new Fields(StormSamoaUtils.CONTENT_EVENT_FIELD, StormSamoaUtils.KEY_FIELD));
            // }
            declarer.declareStream(outputStream.getOutputId(), new Fields(StormSamoaUtils.CONTENT_EVENT_FIELD, StormSamoaUtils.KEY_FIELD));
        }

        StormStream createStream(String piId) {
            // StormSpoutStream stream = new StormSpoutStream(piId);
            StormStream stream = new StormBoltStream(piId);
            // streams.add(stream);
            return stream;
        }

        // void put(StormSpoutStream stream, ContentEvent contentEvent) {
        // tupleInfoQueue.add(new StormTupleInfo(stream, contentEvent));
        // }

        private Values newValues(ContentEvent contentEvent) {
            return new Values(contentEvent, contentEvent.getKey());
        }

        // private final static class StormTupleInfo {
        //
        // private final StormStream stream;
        // private final ContentEvent event;
        //
        // StormTupleInfo(StormStream stream, ContentEvent event) {
        // this.stream = stream;
        // this.event = event;
        // }
        //
        // public StormStream getStormStream() {
        // return this.stream;
        // }
        //
        // public ContentEvent getContentEvent() {
        // return this.event;
        // }
        // }

        // private final static class SpoutStarter implements Runnable {
        //
        // private final TopologyStarter topoStarter;
        //
        // SpoutStarter(TopologyStarter topoStarter) {
        // this.topoStarter = topoStarter;
        // }
        //
        // @Override
        // public void run() {
        // this.topoStarter.start();
        // }
        // }
    }
}
