# frozen_string_literal: true

require "schema_registry_client/schema/base"
require "schema_registry_client/avro_schema_store"

module SchemaRegistry
  module Schema
    class Avro < Base
      DEFAULT_SCHEMAS_PATH = "./schemas"

      def self.schema_type
        "AVRO"
      end

      # @param schema_store [SchemaRegistry::AvroSchemaStore, nil]
      def initialize(schema_store: nil)
        @schema_store = schema_store
      end

      # @return [SchemaRegistry::AvroSchemaStore]
      def schema_store
        @schema_store ||= SchemaRegistry::AvroSchemaStore.new(
          path: SchemaRegistry.avro_schema_path || DEFAULT_SCHEMAS_PATH
        )
        unless @schemas_loaded
          @schema_store.load_schemas!
          @schemas_loaded = true
        end
        @schema_store
      end

      def schema_text(_message, schema_name: nil)
        schema_store.find_text(schema_name)
      end

      def encode(message, stream, schema_name: nil)
        validate_options = {recursive: true,
                            encoded: false,
                            fail_on_extra_fields: true}
        schema = schema_store.find(schema_name)

        ::Avro::SchemaValidator.validate!(schema, message, **validate_options)

        writer = ::Avro::IO::DatumWriter.new(schema)
        encoder = ::Avro::IO::BinaryEncoder.new(stream)
        writer.write(message, encoder)
      end

      def decode(stream, schema_text)
        # Cache parsed writer schemas to avoid re-parsing on every decode
        @parsed_writers_schemas ||= {}
        @parsed_writers_schemas[schema_text] ||= ::Avro::Schema.parse(schema_text)
        writers_schema = @parsed_writers_schemas[schema_text]
        decoder = ::Avro::IO::BinaryDecoder.new(stream)

        # Try to find the reader schema locally, fall back to writer schema
        readers_schema = begin
          schema_store.find(writers_schema.fullname)
        rescue
          writers_schema
        end

        reader = ::Avro::IO::DatumReader.new(writers_schema, readers_schema)
        reader.read(decoder)
      end
    end
  end
end
