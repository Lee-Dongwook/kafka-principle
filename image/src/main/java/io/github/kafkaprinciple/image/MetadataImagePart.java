package io.github.kafkaprinciple.image;

import io.github.kafkaprinciple.image.writer.ImageWriter;
import io.github.kafkaprinciple.image.writer.ImageWriterOptions;

/** Shared temporary implementation for unfinished metadata image sections. */
abstract class MetadataImagePart {
    public boolean isEmpty() { return true; }
    public void write(ImageWriter writer) { /* TODO */ }
    public void write(ImageWriter writer, ImageWriterOptions options) { /* TODO */ }
}
