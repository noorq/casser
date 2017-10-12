package net.helenus.core.cache;

public class Facet {}
/*

An Entity is identifiable via one or more Facets

A Facet is is a set of Properties and bound Facets

An Entity will have it's Keyspace, Table and Schema Version Facets bound.

A property may also have a TTL or write time bound.

The cache contains key->value mappings of merkel-hash -> Entity or Set<Entity>
The only way a Set<Entity> is put into the cache is with a key = hash([Entity's bound Facets, hash(filter clause from SELECT)])

REMEMBER to update the cache on build() for all impacted facets, delete existing keys and add new keys


 */
