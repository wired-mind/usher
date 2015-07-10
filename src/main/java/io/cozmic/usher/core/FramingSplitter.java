package io.cozmic.usher.core;

/**
 * Marker interface which indicates that a Splitter is a framing splitter and by default should deliver bytes
 * to the PipelinePack's msgBytes field. The concept is that if data is framed it is "internal" and it is expected
 * that Decoders will decode the bytes overriding the default PipelinePack message field using setMessage()
 */
public interface FramingSplitter {
}
