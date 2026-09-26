package io.github.kafkaprinciple.common.record.internal;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CompressionRatioEstimator {
    public static final float COMPRESSION_RATIO_IMPROVING_STEP = 0.005f;
    public static final float COMPRESSION_RATIO_DETERIORATE_STEP = 0.05f;
    private static final ConcurrentMap<String, float[]> COMPRESSION_RATIO = new ConcurrentHashMap<>();

    public static float updateEstimation(String topic, CompressionType type, float observedRatio) {
        float[] compressionRatioForTopic = getAndCreateEstimationIfAbsent(topic);
        float currentEstimation = compressionRatioForTopic[type.id];

        synchronized (compressionRatioForTopic) {
            if (observedRatio > currentEstimation) {
                compressionRatioForTopic[type.id] = Math.max(currentEstimation + COMPRESSION_RATIO_DETERIORATE_STEP,
                        observedRatio);
            } else if (observedRatio < currentEstimation) {
                compressionRatioForTopic[type.id] = Math.max(currentEstimation - COMPRESSION_RATIO_IMPROVING_STEP,
                        observedRatio);
            }
        }

        return compressionRatioForTopic[type.id];
    }

    public static float estimation(String topic, CompressionType type) {
        float[] compressionRatioForTopic = getAndCreateEstimationIfAbsent(topic);
        return compressionRatioForTopic[type.id];
    }

    public static void resetEstimation(String topic) {
        float[] compressionRatioForTopic = getAndCreateEstimationIfAbsent(topic);
        synchronized (compressionRatioForTopic) {
            for (CompressionType type : CompressionType.values()) {
                compressionRatioForTopic[type.id] = type.rate;
            }
        }
    }

    public static void setEstimation(String topic, CompressionType type, float ratio) {
        float[] compressionRatioForTopic = getAndCreateEstimationIfAbsent(topic);
        synchronized (compressionRatioForTopic) {
            compressionRatioForTopic[type.id] = ratio;
        }
    }

    private static float[] getAndCreateEstimationIfAbsent(String topic) {
        float[] compressionRatioForTopic = COMPRESSION_RATIO.get(topic);
        if (compressionRatioForTopic == null) {
            compressionRatioForTopic = initialCompressionRatio();
            float[] existingCompressionRatio = COMPRESSION_RATIO.putIfAbsent(topic, compressionRatioForTopic);
            if (existingCompressionRatio != null)
                return existingCompressionRatio;
        }
        return compressionRatioForTopic;
    }

    private static float[] initialCompressionRatio() {
        float[] compressionRatio = new float[CompressionType.values().length];
        for (CompressionType type : CompressionType.values()) {
            compressionRatio[type.id] = type.rate;
        }
        return compressionRatio;
    }
}
