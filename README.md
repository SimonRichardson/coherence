# Coherence

Coherence repository

## Badges

[![travis-ci](https://travis-ci.org/trussle/coherence.svg?branch=master)](https://travis-ci.org/trussle/coherence)
[![Coverage Status](https://coveralls.io/repos/github/trussle/coherence/badge.svg?branch=master)](https://coveralls.io/github/trussle/coherence?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/trussle/coherence)](https://goreportcard.com/report/github.com/trussle/coherence)

## Introduction

Coherence is a cache coherence application for sharing uniformity through
repositories of data. Under the hood this uses a gossip protocol to transfer
data between cache nodes.

```
          +---------+
          |         |
+--------->  Cache  +--------->
          |         |
          +---+-^---+
              | |
              | |
          +---v-+---+
          |         |
+--------->  Cache  +--------->
          |         |
          +---------+
```

In it's current guise it's using replicated objects, but in the future, the 
buckets of data will use a CRDT to give consistency across the nodes. The 
coherence is useful for when sharing information can provide insights to other
caches, but don't necessarily require the cache to be "live" all the time.

This system is an experiment and may not ever pay off!
