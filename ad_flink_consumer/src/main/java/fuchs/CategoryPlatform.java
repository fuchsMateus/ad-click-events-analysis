package fuchs;

public class CategoryPlatform {
    private String platform;
    private String category;
    private long click_count;

    public CategoryPlatform() {
    }

    public CategoryPlatform(String platform, String category, long click_count) {
        this.platform = platform;
        this.category = category;
        this.click_count = click_count;
    }

    public String getPlatform() {
        return platform;
    }

    public String getCategory() {
        return category;
    }

    public long getClickCount() {
        return click_count;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public void setClickCount(long click_count) {
        this.click_count = click_count;
    }
}
