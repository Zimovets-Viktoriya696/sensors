/**
 * Created by vika on 14.05.17.
 */
public class Period {
    private long left;
    private long right;
    private  boolean direction;



    public Period(long left, long right, boolean direction) {
        this.left = left;
        this.right = right;
        this.direction = direction;
    }


    public long getLeft() {
        return left;
    }

    public void setLeft(long left) {
        this.left = left;
    }

    public long getRight() {
        return right;
    }

    public void setRight(long right) {
        this.right = right;
    }

    public boolean isDirection() {
        return direction;
    }

    public void setDirection(boolean direction) {
        this.direction = direction;
    }
}
