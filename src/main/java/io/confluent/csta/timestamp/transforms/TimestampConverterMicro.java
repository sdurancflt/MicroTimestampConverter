package io.confluent.csta.timestamp.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;


import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;

public abstract class TimestampConverterMicro<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Convert timestamps between different formats such as Unix epoch, strings, and Connect Date/Timestamp types."
                    + "Applies to individual fields or to the entire value."
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + TimestampConverterMicro.Key.class.getName() + "</code>) "
                    + "or value (<code>" + TimestampConverterMicro.Value.class.getName() + "</code>).";

    public static final String FIELD_CONFIG = "field";
    private static final String FIELD_DEFAULT = "";

    public static final String TARGET_TYPE_CONFIG = "target.type";

    public static final String FORMAT_CONFIG = "format";
    private static final String FORMAT_DEFAULT = "";

    public static final String ZONE_CONFIG = "timezone";
    private static final String ZONE_DEFAULT = "UTC";

    public static final String UNIX_PRECISION_CONFIG = "unix.precision";
    private static final String UNIX_PRECISION_DEFAULT = "milliseconds";

    private static final String PURPOSE = "converting timestamp formats";

    private static final String TYPE_STRING = "string";
    private static final String TYPE_UNIX = "unix";
    private static final String TYPE_DATE = "Date";
    private static final String TYPE_TIME = "Time";
    private static final String TYPE_TIMESTAMP = "Timestamp";

    private static final String UNIX_PRECISION_MILLIS = "milliseconds";
    private static final String UNIX_PRECISION_MICROS = "microseconds";
    private static final String UNIX_PRECISION_NANOS = "nanoseconds";
    private static final String UNIX_PRECISION_SECONDS = "seconds";

    public static final Schema OPTIONAL_DATE_SCHEMA = org.apache.kafka.connect.data.Date.builder().optional().schema();
    public static final Schema OPTIONAL_TIMESTAMP_SCHEMA = Timestamp.builder().optional().schema();
    public static final Schema OPTIONAL_TIME_SCHEMA = Time.builder().optional().schema();

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_CONFIG, ConfigDef.Type.STRING, FIELD_DEFAULT, ConfigDef.Importance.HIGH,
                    "The field containing the timestamp, or empty if the entire value is a timestamp")
            .define(TARGET_TYPE_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.ValidString.in(TYPE_STRING, TYPE_UNIX, TYPE_DATE, TYPE_TIME, TYPE_TIMESTAMP),
                    ConfigDef.Importance.HIGH,
                    "The desired timestamp representation: string, unix, Date, Time, or Timestamp")
            .define(FORMAT_CONFIG, ConfigDef.Type.STRING, FORMAT_DEFAULT, ConfigDef.Importance.MEDIUM,
                    "A DateTimeFormatter-compatible format for the timestamp. Used to generate the output when type=string "
                            + "or used to parse the input if the input is a string.")
            .define(UNIX_PRECISION_CONFIG, ConfigDef.Type.STRING, UNIX_PRECISION_DEFAULT,
                    ConfigDef.ValidString.in(
                            UNIX_PRECISION_NANOS, UNIX_PRECISION_MICROS,
                            UNIX_PRECISION_MILLIS, UNIX_PRECISION_SECONDS),
                    ConfigDef.Importance.LOW,
                    "The desired Unix precision for the timestamp: seconds, milliseconds, microseconds, or nanoseconds. " +
                            "Used to generate the output when type=unix or used to parse the input if the input is a Long.")
            .define(ZONE_CONFIG, ConfigDef.Type.STRING, ZONE_DEFAULT, ConfigDef.Importance.LOW,
                    "The time zone to use when parsing or formatting timestamps. Defaults to UTC.");

    private interface TimestampTranslator {
        /**
         * Convert from the type-specific format to the universal java.util.Date format
         */
        java.sql.Timestamp toRaw(Config config, Object orig);

        /**
         * Get the schema for this format.
         */
        Schema typeSchema(boolean isOptional);

        /**
         * Convert from the universal java.util.Date format to the type-specific format
         */
        Object toType(Config config,  java.sql.Timestamp orig);
    }

    private static final Map<String, TimestampTranslator> TRANSLATORS = new HashMap<>();
    static {
        TRANSLATORS.put(TYPE_STRING, new TimestampTranslator() {
            @Override
            public java.sql.Timestamp toRaw(Config config, Object orig) {
                if (!(orig instanceof String))
                    throw new DataException("Expected string timestamp to be a String, but found " + orig.getClass());
                try {
                    return java.sql.Timestamp.from(LocalDateTime.parse((String) orig,  config.format).
                            atZone(ZoneId.of(ZONE_DEFAULT)).toInstant());
                } catch (DateTimeParseException e) {
                    throw new DataException("Could not parse timestamp: value (" + orig + ") does not match pattern ("
                            + config.format.toFormat() + ")", e);
                }
            }

            @Override
            public Schema typeSchema(boolean isOptional) {
                return isOptional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
            }

            @Override
            public String toType(Config config,  java.sql.Timestamp orig) {
                synchronized (config.format) {
                    return config.format.format(orig.toInstant());
                }
            }
        });

        TRANSLATORS.put(TYPE_UNIX, new TimestampTranslator() {
            @Override
            public java.sql.Timestamp toRaw(Config config, Object orig) {
                if (!(orig instanceof Long))
                    throw new DataException("Expected Unix timestamp to be a Long, but found " + orig.getClass());
                Long unixTime = (Long) orig;
                switch (config.unixPrecision) {
                    case UNIX_PRECISION_SECONDS:
                        return java.sql.Timestamp.from(Instant.ofEpochSecond(
                                TimeUnit.MICROSECONDS.toSeconds(unixTime),
                                0
                        ));
                    case UNIX_PRECISION_MICROS:
                        return  java.sql.Timestamp.from(Instant.ofEpochSecond(
                                TimeUnit.MICROSECONDS.toSeconds(unixTime),
                                TimeUnit.MICROSECONDS.toNanos(
                                        Math.floorMod(unixTime, TimeUnit.SECONDS.toMicros(1))
                                )
                        ));
                    case UNIX_PRECISION_NANOS:
                        return  java.sql.Timestamp.from(Instant.ofEpochSecond(0L, unixTime));
                    case UNIX_PRECISION_MILLIS:
                    default:
                        return  java.sql.Timestamp.from(Instant.ofEpochMilli(
                                TimeUnit.NANOSECONDS.toMillis(unixTime)
                        ));
                }
            }

            @Override
            public Schema typeSchema(boolean isOptional) {
                return isOptional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
            }

            @Override
            public Long toType(Config config,  java.sql.Timestamp orig) {
                Long unixTimeNano = orig.toInstant().truncatedTo(ChronoUnit.SECONDS).
                        toEpochMilli() * 1_000_000+orig.toInstant().getNano();
                switch (config.unixPrecision) {
                    case UNIX_PRECISION_SECONDS:
                        return TimeUnit.NANOSECONDS.toSeconds(unixTimeNano);
                    case UNIX_PRECISION_MICROS:
                        return TimeUnit.NANOSECONDS.toMicros(unixTimeNano);
                    case UNIX_PRECISION_NANOS:
                        return unixTimeNano;
                    case UNIX_PRECISION_MILLIS:
                    default:
                        return TimeUnit.NANOSECONDS.toMillis(unixTimeNano);
                }
            }
        });

        TRANSLATORS.put(TYPE_DATE, new TimestampTranslator() {
            @Override
            public  java.sql.Timestamp toRaw(Config config, Object orig) {
                if (!(orig instanceof Date))
                    throw new DataException("Expected Instant to be a java.util.Date, but found " + orig.getClass());
                // Already represented as a java.time.Instant
                return  (java.sql.Timestamp) orig;
            }

            @Override
            public Schema typeSchema(boolean isOptional) {
                return isOptional ? OPTIONAL_DATE_SCHEMA : org.apache.kafka.connect.data.Date.SCHEMA;
            }

            @Override
            public java.sql.Timestamp toType(Config config,  java.sql.Timestamp orig) {
                return orig;
            }
        });

        TRANSLATORS.put(TYPE_TIME, new TimestampTranslator() {
            @Override
            public java.sql.Timestamp toRaw(Config config, Object orig) {
                if (!(orig instanceof java.sql.Timestamp))
                    throw new DataException("Expected Time to be a java.sql.Timestamp, but found " + orig.getClass());
                return (java.sql.Timestamp)orig;
            }

            @Override
            public Schema typeSchema(boolean isOptional) {
                return isOptional ? OPTIONAL_TIME_SCHEMA : Time.SCHEMA;
            }

            @Override
            public java.sql.Timestamp toType(Config config, java.sql.Timestamp orig) {
                return orig;
            }
        });

        TRANSLATORS.put(TYPE_TIMESTAMP, new TimestampTranslator() {
            @Override
            public java.sql.Timestamp toRaw(Config config, Object orig) {
                if (!(orig instanceof java.sql.Timestamp))
                    throw new DataException("Expected Timestamp to be a java.sql.Timestamp, but found " + orig.getClass());
                return (java.sql.Timestamp) orig;
            }

            @Override
            public Schema typeSchema(boolean isOptional) {
                return isOptional ? OPTIONAL_TIMESTAMP_SCHEMA : Timestamp.SCHEMA;
            }

            @Override
            public java.sql.Timestamp toType(Config config, java.sql.Timestamp orig) {
                return orig;
            }
        });
    }

    // This is a bit unusual, but allows the transformation config to be passed to static anonymous classes to customize
    // their behavior
    private static class Config {
        Config(String field, String type, DateTimeFormatter format, String unixPrecision) {
            this.field = field;
            this.type = type;
            this.format = format;
            this.unixPrecision = unixPrecision;
        }
        String field;
        String type;
        DateTimeFormatter format;
        String unixPrecision;
    }
    private Config config;
    private Cache<Schema, Schema> schemaUpdateCache;


    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig simpleConfig = new SimpleConfig(CONFIG_DEF, configs);
        final String field = simpleConfig.getString(FIELD_CONFIG);
        final String type = simpleConfig.getString(TARGET_TYPE_CONFIG);
        String formatPattern = simpleConfig.getString(FORMAT_CONFIG);
        final String unixPrecision = simpleConfig.getString(UNIX_PRECISION_CONFIG);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));

        if (type.equals(TYPE_STRING) && Utils.isBlank(formatPattern)) {
            throw new ConfigException("TimestampConverter requires format option to be specified when using string timestamps");
        }
        DateTimeFormatter format = null;
        if (!Utils.isBlank(formatPattern)) {
            try {
                format = DateTimeFormatter.ofPattern(formatPattern).withZone(ZoneId.of(ZONE_DEFAULT));
            } catch (IllegalArgumentException e) {
                throw new ConfigException("TimestampConverter requires a DateTimeFormatter-compatible pattern for string timestamps: "
                        + formatPattern, e);
            }
        }
        config = new Config(field, type, format, unixPrecision);
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    public static class Key<R extends ConnectRecord<R>> extends TimestampConverterMicro<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends TimestampConverterMicro<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    private R applyWithSchema(R record) {
        final Schema schema = operatingSchema(record);
        if (config.field.isEmpty()) {
            Object value = operatingValue(record);
            // New schema is determined by the requested target timestamp type
            Schema updatedSchema = TRANSLATORS.get(config.type).typeSchema(schema.isOptional());
            return newRecord(record, updatedSchema, convertTimestamp(value, timestampTypeFromSchema(schema)));
        } else {
            final Struct value = requireStructOrNull(operatingValue(record), PURPOSE);
            Schema updatedSchema = schemaUpdateCache.get(schema);
            if (updatedSchema == null) {
                SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
                for (Field field : schema.fields()) {
                    if (field.name().equals(config.field)) {
                        builder.field(field.name(), TRANSLATORS.get(config.type).typeSchema(field.schema().isOptional()));
                    } else {
                        builder.field(field.name(), field.schema());
                    }
                }
                if (schema.isOptional())
                    builder.optional();
                if (schema.defaultValue() != null) {
                    Struct updatedDefaultValue = applyValueWithSchema((Struct) schema.defaultValue(), builder);
                    builder.defaultValue(updatedDefaultValue);
                }

                updatedSchema = builder.build();
                schemaUpdateCache.put(schema, updatedSchema);
            }

            Struct updatedValue = applyValueWithSchema(value, updatedSchema);
            return newRecord(record, updatedSchema, updatedValue);
        }
    }

    private Struct applyValueWithSchema(Struct value, Schema updatedSchema) {
        if (value == null) {
            return null;
        }
        Struct updatedValue = new Struct(updatedSchema);
        for (Field field : value.schema().fields()) {
            final Object updatedFieldValue;
            if (field.name().equals(config.field)) {
                updatedFieldValue = convertTimestamp(value.get(field), timestampTypeFromSchema(field.schema()));
            } else {
                updatedFieldValue = value.get(field);
            }
            updatedValue.put(field.name(), updatedFieldValue);
        }
        return updatedValue;
    }

    private R applySchemaless(R record) {
        Object rawValue = operatingValue(record);
        if (rawValue == null || config.field.isEmpty()) {
            return newRecord(record, null, convertTimestamp(rawValue));
        } else {
            final Map<String, Object> value = requireMap(rawValue, PURPOSE);
            final HashMap<String, Object> updatedValue = new HashMap<>(value);
            updatedValue.put(config.field, convertTimestamp(value.get(config.field)));
            return newRecord(record, null, updatedValue);
        }
    }

    /**
     * Determine the type/format of the timestamp based on the schema
     */
    private String timestampTypeFromSchema(Schema schema) {
        if (Timestamp.LOGICAL_NAME.equals(schema.name())) {
            return TYPE_TIMESTAMP;
        } else if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(schema.name())) {
            return TYPE_DATE;
        } else if (Time.LOGICAL_NAME.equals(schema.name())) {
            return TYPE_TIME;
        } else if (schema.type().equals(Schema.Type.STRING)) {
            // If not otherwise specified, string == user-specified string format for timestamps
            return TYPE_STRING;
        } else if (schema.type().equals(Schema.Type.INT64)) {
            // If not otherwise specified, long == unix time
            return TYPE_UNIX;
        }
        throw new ConnectException("Schema " + schema + " does not correspond to a known timestamp type format");
    }

    /**
     * Infer the type/format of the timestamp based on the raw Java type
     */
    private String inferTimestampType(Object timestamp) {
        if (timestamp instanceof Date) {
            return TYPE_TIMESTAMP;
        } else if (timestamp instanceof Long) {
            return TYPE_UNIX;
        } else if (timestamp instanceof String) {
            return TYPE_STRING;
        }
        throw new DataException("TimestampConverter does not support " + timestamp.getClass() + " objects as timestamps");
    }

    /**
     * Convert the given timestamp to the target timestamp format.
     * @param timestamp the input timestamp, may be null
     * @param timestampFormat the format of the timestamp, or null if the format should be inferred
     * @return the converted timestamp
     */
    private Object convertTimestamp(Object timestamp, String timestampFormat) {
        if (timestamp == null) {
            return null;
        }
        if (timestampFormat == null) {
            timestampFormat = inferTimestampType(timestamp);
        }

        TimestampTranslator sourceTranslator = TRANSLATORS.get(timestampFormat);
        if (sourceTranslator == null) {
            throw new ConnectException("Unsupported timestamp type: " + timestampFormat);
        }
        java.sql.Timestamp rawTimestamp = sourceTranslator.toRaw(config, timestamp);
        TimestampTranslator targetTranslator = TRANSLATORS.get(config.type);
        if (targetTranslator == null) {
            throw new ConnectException("Unsupported timestamp type: " + config.type);
        }
        return targetTranslator.toType(config, rawTimestamp);
    }

    private Object convertTimestamp(Object timestamp) {
        return convertTimestamp(timestamp, null);
    }
}
