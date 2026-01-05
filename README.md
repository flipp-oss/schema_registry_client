# schema_registry_client

`schema_registry_client` is a library to interact with the Confluent Schema Registry. It is inspired by and based off of [avro_turf](https://github.com/dasch/avro_turf).

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'schema_registry_client'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install schema_registry_client

## Usage

SchemaRegistry interacts with the Confluent Schema Registry, and caches all results. When you first encode a message, it will register the message and all dependencies with the Schema Registry. When decoding, it will look up the schema in the Schema Registry and use the associated local generated code to decode the message.

Example usage:

### Avro

Note that unlike AvroTurf, you must concatenate the namespace and schema name together when encoding.

```ruby
require 'schema_registry_client'

client = SchemaRegistry::Client.new(registry_url: 'http://localhost:8081', schema_type: SchemaRegistry::Schema::Avro)
SchemaRegistry.avro_schema_path = 'path/to/schemas'
message = {field1: 'value1', field2: 42 }
encoded = client.encode(message, schema_name: 'com.my-namespace.MySchema', subject: 'my-subject')

# Decoding

decoded_avro_message = client.decode(encoded_string)
```

### Protobuf

```ruby
client = SchemaRegistry::Client.new(registry_url: 'http://localhost:8081', schema_type: SchemaRegistry::Schema::Protobuf)
message = MyProtoMessage.new(field1: 'value1', field2: 42)
encoded = client.encode(message, subject: 'my-proto-subject')

# Decoding
decoded_proto_message = client.decode(encoded_string)
```

### Protobuf JSON Schema

Since Protobuf is [not recommended to use for Kafka keys](https://protobuf.dev/programming-guides/encoding/#implications), it's instead recommended that you use the `ProtoJsonSchema` format. This will transform the Protobuf message into a JSON schema and register that with the registry instead. The encoded message will be the JSON representation of the Protobuf message, with keys sorted alphabetically to ensure consistent encoding.

Note that the algorithm to translate into JSON Schema is currently very naive (e.g. it does not handle nested messages) since keys should usually be very simple. If more complex logic is needed, pull requests are welcome.

```ruby
client = SchemaRegistry::Client.new(registry_url: 'http://localhost:8081', schema_type: SchemaRegistry::Schema::ProtoJsonSchema)
message = MyProtoMessage.new(field1: 'value1', field2: 42)
encoded = client.encode(message, subject: 'my-proto-subject') # will register a JSON Schema subject and encode into JSON

```

You can use JSON Schema with regular Ruby hashes as well by passing `schema_text` into the encode method:

```ruby
client = SchemaRegistry::Client.new(registry_url: 'http://localhost:8081', schema_type: SchemaRegistry::Schema::ProtoJsonSchema)
message = { field1: 'value1', field2: 42 }
schema_text = {
    "type" => "object",
    "properties" => {
        "field1" => { "type" => "string" },
        "field2" => { "type" => "integer" }
    },
    "required" => ["field1", "field2"]
}.to_json
encoded = client.encode(message, subject: 'my-proto-subject', schema_text: schema_text)
```

## Notes about usage

* When decoding, this library does *not* attempt to fully parse the Protobuf definition stored on the schema registry and generate dynamic classes. Instead, it simply parses out the package and message and assumes that the reader has the message available in the descriptor pool. Any compatibility issues should be detected through normal means, i.e. just by instantiating the message and seeing if any errors are raised.

### Regenerating test protos
Run the following to regenerate:

```sh
protoc -I spec/schemas --ruby_out=spec/gen --ruby_opt=paths=source_relative spec/schemas/**/*.proto
```
