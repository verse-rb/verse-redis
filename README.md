# Verse::Redis

This gem provides a Redis-backed implementation for several `verse-core` modules, including:

* **Event Manager:** A Redis Streams-based event manager.
* **Distributed Utilities:**
    * **KVStore:** A distributed key-value store.
    * **Lock:** A distributed lock manager.
    * **Counter:** A distributed atomic counter.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'verse-redis'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install verse-redis

## Usage

### Plugin Configuration

To use this gem, you need to configure the `verse-redis` plugin in your `config.ru` or an initializer. You can configure multiple instances of the plugin to connect to different Redis databases.

```ruby
# config/verse.rb

Verse.start(
  # ...
  config_path: "./config"
)
```

```yaml
# config/development.yml

plugins:
  - name: redis
    config:
      url: "redis://localhost:6379/0"
      max_connections: 5

  - name: redis_cache
    plugin: redis
    config:
      url: "redis://localhost:6379/1"
      max_connections: 10
```

### Event Manager

To use the Redis-backed event manager, configure it in your `config.ru` or an initializer:

```yaml
# config/development.yml

em:
  adapter: Verse::Redis::Stream::EventManager
  config:
    redis_plugin: :redis # optional, default to :redis
    # other event manager options
```

### Distributed Utilities

The distributed utilities can be configured to use the `verse-redis` plugin.

#### KVStore

To use the Redis-backed KVStore, configure it in your `config.ru` or an initializer:

```yaml
# config/development.yml

kv_store:
  adapter: Verse::Redis::KVStore
  config:
    plugin: :redis # optional, default to :redis
    key_prefix: "my_app" # optional
```

#### Lock

To use the Redis-backed Lock, configure it in your `config.ru` or an initializer:

```yaml
# config/development.yml

lock:
  adapter: Verse::Redis::Lock
  config:
    plugin: :redis # optional, default to :redis
    key_prefix: "my_app:locks" # optional
```

#### Counter

To use the Redis-backed Counter, configure it in your `config.ru` or an initializer:

```yaml
# config/development.yml

counter:
  adapter: Verse::Redis::Counter
  config:
    plugin: :redis # optional, default to :redis
    key_prefix: "my_app:counters" # optional
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`.

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/verse-rb/verse-redis.
