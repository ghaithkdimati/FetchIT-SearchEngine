package engine.searchengine.Models;

import java.io.Serializable;

public class ImageObject implements Serializable {

    private final String page;
    private final String alt;
    private final String src;

    public ImageObject(String page, String alt, String src) {
        this.page = page;
        this.alt = alt;
        this.src = src;
    }

    public String getPage() {
        return page;
    }

    public String getAlt() {
        return alt;
    }

    public String getSrc() {
        return src;
    }


}
