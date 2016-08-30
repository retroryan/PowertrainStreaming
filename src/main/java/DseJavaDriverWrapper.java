/**
 * Created by peyton on 5/30/16.
 */

import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.graph.GraphOptions;

public class DseJavaDriverWrapper {
    public DseJavaDriverWrapper(){

    }
    public DseCluster CreateNewCluster(String dse_host, String graph_name)
    {
        DseCluster dseCluster;

        if(graph_name != "") {
            dseCluster = new DseCluster.Builder()
                    .addContactPoint(dse_host)
                    .withGraphOptions(new GraphOptions().setGraphName(graph_name))
                    .build();
        }
        else
        {
            dseCluster = new DseCluster.Builder()
                    .addContactPoint(dse_host)
                    .build();
        }
        return dseCluster;
    }
}
