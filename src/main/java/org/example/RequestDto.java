package org.example;

import java.util.List;
import java.util.UUID;

public class RequestDto {

    private List<UUID> ids;

    private String type;

    public RequestDto(List<UUID> ids, String type) {
        this.ids = ids;
        this.type = type;
    }

    public List<UUID> getIds() {
        return ids;
    }

    public void setIds(List<UUID> ids) {
        this.ids = ids;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
