# Coherence

Fully distributed Key/Value Store.

## Badges

[![travis-ci](https://travis-ci.org/SimonRichardson/coherence.svg?branch=master)](https://travis-ci.org/SimonRichardson/coherence)
[![Coverage Status](https://coveralls.io/repos/github/SimonRichardson/coherence/badge.svg?branch=master)](https://coveralls.io/github/SimonRichardson/coherence?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/SimonRichardson/coherence)](https://goreportcard.com/report/github.com/SimonRichardson/coherence)

## Introduction

Coherence is a limited Key/Value store, that aims to provide high availability, 
with eventual consistency. The limited aspect of the store is provided by a
LRU so that it can provide a windowed data set, that from the outset guarantees 
to fit into smaller confined spaces.
