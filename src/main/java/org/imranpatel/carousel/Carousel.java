package org.imranpatel.carousel;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.RiakCommand;
import com.basho.riak.client.api.commands.FetchSet;
import com.basho.riak.client.api.commands.UpdateSet;
import com.basho.riak.client.api.commands.datatypes.Context;
import com.basho.riak.client.api.commands.datatypes.SetUpdate;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.util.BinaryValue;
import com.google.common.collect.Ordering;
import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class Carousel {
    private RiakClient client;
    private String name;
    private int shardCount;
    private List<Location> shards;

    private static final Gson gson = new Gson();
    private static final Ordering<BinaryValue> timeOrdering = new Ordering<BinaryValue>() {
        public int compare(BinaryValue left, BinaryValue right) {
            return (int) (Item.fromJson(left.toString()).getCreatedAtMillis()
                    - Item.fromJson(right.toString()).getCreatedAtMillis());
        }
    };

    public Carousel(RiakClient client, String name, int shardCount) {
        this.client = client;
        this.name = name;
        this.shardCount = shardCount;

        shards = new ArrayList<Location>(shardCount);
        for (int i = 0; i < shardCount; i++) {
             shards.add(new Location(new Namespace("carousel", name), name + i));
        }
    }

    public void add(String id, String payload) throws CarouselException {
        Item item = new Item(id, payload);
        SetUpdate su = new SetUpdate().add(BinaryValue.create(item.toJson()));
        UpdateSet update = new UpdateSet.Builder(getShard(id), su).build();
        riakExecute(update);
    }

    public void remove(Item item) throws CarouselException {
        Location shard = getShard(item.getId());
        FetchSet fetch = new FetchSet.Builder(shard).build();
        FetchSet.Response response = riakExecute(fetch);
        Context context = response.getContext();
        SetUpdate su = new SetUpdate().remove(BinaryValue.create(item.toJson()));
        UpdateSet update = new UpdateSet.Builder(shard, su).withContext(context).build();
        riakExecute(update);
    }

    public Item get() throws CarouselException {
        for (Location shard : shards) {
            FetchSet fetch = new FetchSet.Builder(shard).build();
            FetchSet.Response response = riakExecute(fetch);
            Set<BinaryValue> binarySet = response.getDatatype().viewAsSet();
            if (binarySet.isEmpty()) {
                continue;
            }

            BinaryValue value = timeOrdering.min(binarySet);
            return Item.fromJson(value.toString());
        }
        return null;
    }

    private Location getShard(String id){
        return shards.get(Math.abs(id.hashCode() % shardCount));
    }

    private <T, S> T riakExecute(RiakCommand<T, S> command) {
        try {
            return client.execute(command);
        } catch (ExecutionException e) {
            throw new CarouselException(e);
        } catch (InterruptedException e) {
            throw new CarouselException(e);
        }
    }

    public static class Item {
        private String id;
        private String payload;
        private long createdAtMillis;

        public Item(String id, String payload) {
            this.id = id;
            this.payload = payload;
            this.createdAtMillis = new Date().getTime();
        }

        public static Item fromJson(String json) {
            return gson.fromJson(json, Item.class);
        }

        public String toJson() {
            return gson.toJson(this);
        }

        public String getId() {
            return id;
        }

        public String getPayload() {
            return payload;
        }

        public long getCreatedAtMillis() {
            return createdAtMillis;
        }
    }

    public static class CarouselException extends RuntimeException {
        public CarouselException(Throwable cause) {
            super(cause);
        }
    }

}
