import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.apache.commons.text.similarity.LongestCommonSubsequence;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class JSpell {
    private Trie<String, LogCluster> trie;
    private Node prefixTreeRoot;
    private List<LogCluster> cluster;
    private String logformat;
    private float tau;

    private final LongestCommonSubsequence lcs = new LongestCommonSubsequence();

    private final Logger logger = Logger.getLogger(JSpell.class.getName());

    private Map<String, List> dataframe;
    private Pattern regex;

    public JSpell(String logformat, float tau) {
        this.trie = new PatriciaTrie<>();
        this.prefixTreeRoot = new Node();
        this.cluster = new ArrayList<>();
        this.logformat = logformat;
        this.tau = tau;
//        this.dataframe = new HashMap<>();

        generateLogformatRegex(logformat);
    }

    public void parse(List<String> log){
        loadDataframe(log);
        List<String> content = dataframe.get("Content");

        for (int i=0; i < content.size(); i++){
            int logID = i;
            var msg = Arrays.stream(content.get(i).split("[\\s=:,]"))
                    .filter(x-> !x.isEmpty())
                    .toList();
            var constLogMsg = msg.stream().filter(x -> !x.equals("<*>")).toList();

            var matchCluster = prefixTreeMatch(prefixTreeRoot, constLogMsg, 0);
        }

    }

    private LogCluster[] prefixTreeMatch(Node prefixTree, List<String> constLogMsg, int start) {
        for (int i = start; i < constLogMsg.size(); i++) {
            if (prefixTree.child.containsKey(constLogMsg.get(i))){
                var child = prefixTree.child.get(constLogMsg.get(i));
                if (child.cluster.getTemplate() != null){
                    
                }else
                    prefixTreeMatch(child, constLogMsg, i+1)
            }
        }
        return null;
    }

    private void loadDataframe(List<String> log){
        int count = 0;
        int total = log.size();
//        var messages = new ArrayList<List<String>>();
        for (var l: log) {
            l = l.replaceAll("[^x00-x7F]+", "<NASCII>").strip();
            var match = regex.matcher(l);
            if (match.find()){
                dataframe.forEach((key, list) -> list.add(match.group(key)));
//                for (var h : dataframe.keySet()) {
//                    dataframe.get(h).add(match.group(h));
//                }
//                var message = new ArrayList<String>();
//                for (var h : dataframe.keySet()) {
//                    message.add(match.group(h));
//                }
//                messages.add(message);
            }
            count++;
            if (count % 10000 == 0 || count == total)
                logger.log(Level.INFO, String.format("Loaded %.2f%% of log lines.", 100*count/total));
        }
//        int tot = dataframe.entrySet().iterator().next().getValue().size();
        ArrayList<Integer> numbers = new ArrayList<>(count);
        for(int i = 0; i < count; i++){
            numbers.add(i);
        }
        dataframe.put("LineID", numbers);
    }

    private void generateLogformatRegex(String logformat){
//        Function to generate regular expression to split log messages
        var headers = new ArrayList<String>();
        var splitters = logformat.split("(<[^<>]+>)");
        StringBuilder regex = new StringBuilder();
        for (int k = 0; k < splitters.length; k++){
            if (k % 2 == 0){
                var splitter = splitters[k].replaceAll("\\ +", " ");
                regex.append(splitter);
            }else {
                var header = splitters[k].strip().replace("<|>", "");
                regex.append(String.format("(?P<%s>.*?)",header));
                headers.add(header);
            }
        }
        this.regex = Pattern.compile("^" + regex + "$");
        this.dataframe = new HashMap<>(headers.size());
        for (var h :headers) {
            dataframe.put(h, new ArrayList<String>());
        }
    }



}

/**
 * Class object to store a log group with the same template
 */
class LogCluster{
    String template;

    int id;

    public LogCluster(String template, int id) {
        this.template = template;
        this.id = id;
    }

    public String getTemplate() {
        return template;
    }

    public void setTemplate(String template) {
        this.template = template;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
}

class Node {
    LogCluster cluster;
    String token;
    int templateNo;
    Map<String, Node> child;

    public Node() {
        this.child = new HashMap<>();
    }
}