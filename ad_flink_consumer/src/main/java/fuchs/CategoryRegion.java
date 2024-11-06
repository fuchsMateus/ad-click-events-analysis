package fuchs;

public class CategoryRegion {
    private String location;
    private String category;
    private long click_count;

    public CategoryRegion() {
    }

    public CategoryRegion(String location, String category, long click_count) {
        this.location = location;
        this.category = category;
        this.click_count = click_count;
    }

    public String getLocation() {
        return location;
    }

    public String getCategory() {
        return category;
    }

    public long getClickCount() {
        return click_count;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public void setClickCount(long click_count) {
        this.click_count = click_count;
    }

}
