package GetStarted;

import java.awt.*;
import java.util.HashMap;
import java.util.Map;

public class Vertex {
    private String label;
    private String id;
    private Map<Object, Object> properties;

    public Vertex(){
        properties = new HashMap<>();
    }

    public String getLabel(){
        return label;
    }

    public void setLabel(String label){
        this.label = label;
    }

    public String getId(){
        return id;
    }

    public void setId(String id){
        this.id = id;
    }

    public Map<Object, Object> getProperties(){
        return properties;
    }
}
