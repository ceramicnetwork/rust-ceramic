-- Add up migration script here

/*
 We rely on the event CID as the PK so the recon ID/ordering concept is independent and not understood by other tables.
 
 Chose order_key instead of id/event_id because that's how it is used. We'll define more "orders" in a 
 flat recon world e.g. one that puts controller first so you could find all events by a controller rather
 than by a model. In reality, we would want to filter and then order, rather than order where overlaps of 
 one facet of the range should be included IFF the other filters are met. But that's going to require a 
 recon version bump so we leave it for the future.
*/
CREATE TABLE IF NOT EXISTS "event" (
    order_key BLOB NOT NULL UNIQUE, -- network_id sep_key sep_value controller stream_id event_cid
    ahash_0 INTEGER NOT NULL, -- the ahash is decomposed as [u32; 3]
    ahash_1 INTEGER NOT NULL,
    ahash_2 INTEGER NOT NULL,
    ahash_3 INTEGER NOT NULL,
    ahash_4 INTEGER NOT NULL,
    ahash_5 INTEGER NOT NULL,
    ahash_6 INTEGER NOT NULL,
    ahash_7 INTEGER NOT NULL,
    cid BLOB NOT NULL, -- the cid of the event as bytes no 0x00 prefix
    discovered TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    delivered INTEGER UNIQUE, -- monotonic increasing counter indicating this can be delivered to clients
    PRIMARY KEY(cid)
);
