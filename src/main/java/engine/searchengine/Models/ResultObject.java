package engine.searchengine.Models;

import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Controller;

import java.io.Serializable;


public class ResultObject implements Serializable {
    private final String url;
    private final String title;
    private final String description;
    private final String icon;
    private final String name;

    public ResultObject(String url, String title, String description, String icon, String name) {
        this.url = url;
        this.title = title;
        this.description = description;
        this.icon = icon;
        this.name = name;
    }

    public String getUrl() {
        return url;
    }

    public String getTitle() {
        return title;
    }

    public String getDescription() {
        return description;
    }

    public String getIcon() {
        return icon;
    }

    public String getName() {
        return name;
    }
}
