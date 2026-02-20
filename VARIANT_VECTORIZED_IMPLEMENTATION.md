# Vectorized Variant Reading Implementation

## Overview

This document describes the implementation of vectorized reading support for Iceberg Variant types in the Arrow module.

## Background

### What are Variants?

Variants are a semi-structured data type in Iceberg that can store arbitrary JSON-like data. In Parquet, variants are stored as a GroupType with the following structure:

```
group variant_column (VARIANT) {
  required binary metadata;      // Dictionary of field names and metadata
  optional binary value;          // Serialized variant value (unshreddded)
  optional ... typed_value;       // Shredded/typed representation (not yet supported)
}
```

### Why Vectorized Reading?

Vectorized reading processes multiple rows at once, improving performance by:
- Reducing function call overhead
- Enabling SIMD operations
- Better cache locality
- Amortizing deserialization costs

## Implementation

### Architecture

The implementation consists of three main components:

1. **VectorizedVariantReader** - New reader class that handles variant columns
2. **VectorizedReaderBuilder** - Updated to recognize and create variant readers
3. **TestVectorizedVariantReads** - Test suite for variant vectorized reading

### Component Details

#### 1. VectorizedVariantReader

**Location:** `arrow/src/main/java/org/apache/iceberg/arrow/vectorized/VectorizedVariantReader.java`

**Purpose:** Reads variant data from Parquet files in batches.

**Key Features:**
- Reads metadata and value columns separately
- Combines them into an Arrow StructVector with two children
- Handles the GroupType structure of variants
- Supports batch reading with configurable batch sizes

**Implementation:**
```java
public class VectorizedVariantReader implements VectorizedReader<VectorHolder> {
  private final VectorizedArrowReader metadataReader;  // Reads metadata column
  private final VectorizedArrowReader valueReader;      // Reads value column
  private StructVector variantVector;                   // Output: struct(metadata, value)

  @Override
  public VectorHolder read(VectorHolder reuse, int numValsToRead) {
    // Read metadata and value columns
    // Transfer data to struct vector
    // Return combined result
  }
}
```

#### 2. VectorizedReaderBuilder

**Location:** `arrow/src/main/java/org/apache/iceberg/arrow/vectorized/VectorizedReaderBuilder.java`

**Changes:** Added `variant()` method override

**Purpose:** Creates VectorizedVariantReader when encountering variant columns

**Implementation:**
```java
@Override
public VectorizedReader<?> variant(
    Types.VariantType expected, GroupType variantGroup, VectorizedReader<?> result) {
  // Find metadata and value column descriptors
  String[] metadataPath = path("metadata");
  String[] valuePath = path("value");

  ColumnDescriptor metadataDesc = parquetSchema.getColumnDescription(metadataPath);
  ColumnDescriptor valueDesc = parquetSchema.getColumnDescription(valuePath);

  // Create variant reader with both columns
  return new VectorizedVariantReader(...);
}
```

#### 3. TestVectorizedVariantReads

**Location:** `arrow/src/test/java/org/apache/iceberg/arrow/vectorized/TestVectorizedVariantReads.java`

**Purpose:** Test suite for variant vectorized reading

**Test Cases:**
- `testSerializedVariantPrimitives()` - Tests primitive variant values
- `testSerializedVariantWithMetadata()` - Tests variants with metadata dictionaries
- `testVariantBatchReading()` - Tests reading 1000 variants in batches
- `testVariantWithNulls()` - Tests null handling

**Current Status:** Tests write variant data and verify file creation. Full assertions pending integration with Iceberg's variant conversion logic.

## Current Scope & Limitations

### What's Implemented ✅

- Reading serialized variants (metadata + value columns)
- Batch reading with configurable batch sizes
- Proper handling of variant's GroupType structure
- Test infrastructure for variant reading
- Compiles and integrates with existing vectorized reader architecture

### What's NOT Implemented ❌

- **Shredded variant support** - typed_value column with nested structures (objects, arrays)
- **Variant object conversion** - Converting Arrow StructVector → Iceberg Variant
- **Metadata deduplication** - Optimizing repeated metadata values across batches
- **Dictionary encoding** - Leveraging Parquet dictionary encoding for metadata
- **Full test verification** - Tests currently only verify file creation, not data correctness

## Usage

### Writing Variant Data

```java
// Create variant data
ByteBuffer metadata = createMetadata();
VariantMetadata variantMetadata = Variants.metadata(metadata);
Variant variant = Variant.of(variantMetadata, Variants.of(42));

// Write to Parquet
Record record = GenericRecord.create(schema);
record.setField("variant_col", variant);

Parquet.write(outputFile)
    .schema(schema)
    .createWriterFunc(GenericParquetWriter::create)
    .build()
    .add(record);
```

### Reading Variant Data (Vectorized)

```java
// The vectorized reader is automatically used when:
// 1. Reading a variant column
// 2. Using Arrow-based readers
// 3. Vectorized reading is enabled

// The reader will produce StructVector with:
// - metadata: VarBinaryVector (required)
// - value: VarBinaryVector (optional)
```

## Performance Considerations

### Current Performance

- **Batch Size:** Default 5000 rows (configurable)
- **Memory:** Two VarBinaryVectors per batch (metadata + value)
- **Overhead:** Minimal - delegates to existing VectorizedArrowReader for each column

### Potential Optimizations

1. **Metadata Deduplication:**
   - Variant metadata is often identical across rows
   - Could deduplicate at batch level to save memory
   - Estimated savings: 50-90% for homogeneous data

2. **Dictionary Encoding:**
   - Leverage Parquet dictionary encoding for metadata column
   - Already supported by underlying readers
   - No code changes needed

3. **SIMD Operations:**
   - Arrow vectors support SIMD operations
   - Could benefit from vectorized comparisons/filters
   - Requires integration with Iceberg's expression evaluation

## Next Steps

### Short Term (Required for Production)

1. **Variant Conversion Logic:**
   - Add utility to convert StructVector → List<Variant>
   - Integrate with Iceberg's Variant API
   - Update tests to verify data correctness

2. **Null Handling:**
   - Verify null variant handling
   - Test null metadata vs null value
   - Ensure proper validity bitmaps

3. **Integration Tests:**
   - End-to-end tests with Spark/Flink
   - Performance benchmarks vs non-vectorized
   - Memory profiling

### Medium Term (Performance)

1. **Metadata Optimization:**
   - Implement metadata deduplication
   - Cache metadata across batches
   - Use dictionary encoding where beneficial

2. **Shredded Variant Support:**
   - Handle typed_value column
   - Read nested structures (objects, arrays)
   - Merge shredded + serialized values

### Long Term (Advanced Features)

1. **Predicate Pushdown:**
   - Push variant filters to Parquet
   - Leverage stats and dictionaries
   - Skip non-matching row groups

2. **Projection Pushdown:**
   - Read only requested variant fields
   - Use shredded columns for field access
   - Avoid deserializing full variants

## Testing

### Running Tests

```bash
# Compile tests
./gradlew :iceberg-arrow:compileTestJava

# Run all variant tests
./gradlew :iceberg-arrow:test --tests TestVectorizedVariantReads

# Run specific test
./gradlew :iceberg-arrow:test --tests TestVectorizedVariantReads.testSerializedVariantPrimitives
```

### Test Data

Tests use minimal metadata format:
```
ByteBuffer metadata = ByteBuffer.allocate(4)
  .put((byte) 1)   // version
  .put((byte) 0)   // offset size
  .put((byte) 1)   // sorted fields flag
  .put((byte) 0);  // empty dictionary
```

## References

- [Iceberg Variant Specification](https://github.com/apache/iceberg/blob/main/format/spec.md#variant)
- [Parquet Variant Storage](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#variant)
- [Arrow Vectorization](https://arrow.apache.org/docs/java/vector.html)
- [VectorizedArrowReader](arrow/src/main/java/org/apache/iceberg/arrow/vectorized/VectorizedArrowReader.java)

## Contributors

- Implementation: Claude Opus 4.6
- Review: [Pending]
- Testing: [In Progress]
