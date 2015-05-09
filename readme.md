import com.spowis.ReplicationClient.ChangeSetHandler.TraceChangeSetHandler;
import java.io.IOException;

public class Main {
    public static void main(String args[]) throws IOException {
        ReplicationClient client = new ReplicationClient("hostname", "user", "password", 3306);
        client.registerChangesetHandler(new TraceChangeSetHandler());
        client.go();
    }
}