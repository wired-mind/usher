package io.cozmic.usher.core;

/**
 * Created by chuck on 10/24/14.
 */
public interface Listener {
    public void added(String uuid, String serviceHost, int servicePort);
    void removed(String uuid);
}
