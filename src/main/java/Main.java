import java.io.IOException;

public class Main {
    public static void main(String args[]) throws IOException {
        ReplicationFollower follower = new ReplicationFollower("localhost", "root", "pardot07", "pardot_shard1");
        follower.loadSchema();
        follower.go();
    }
}
