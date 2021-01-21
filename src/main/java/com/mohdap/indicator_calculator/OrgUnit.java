package com.mohdap.indicator_calculator;

/**
 *
 * @author duncanndiithi
 */
public class OrgUnit {

    private String name;
    private int id;
    private int parentId;
    private OrgLevel hierarchylevel;
    private String uuid;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getParentId() {
        return parentId;
    }

    public void setParentId(int parentId) {
        this.parentId = parentId;
    }

    public OrgLevel getHierarchylevel() {
        return hierarchylevel;
    }

    public void setHierarchylevel(OrgLevel hierarchylevel) {
        this.hierarchylevel = hierarchylevel;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

}
