import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class JSpell {
    private Trie<String, LogCluster> trie;
    private TrieNode prefixTreeRoot;
    private List<LogCluster> cluster;
    private String logformat;
    private float tau;

    private final Logger logger = Logger.getLogger(JSpell.class.getName());

    private Map<String, List> dataframe;
    private Pattern regex;

    public JSpell(String logformat, float tau) {
        this.trie = new PatriciaTrie<>();
        this.prefixTreeRoot = new TrieNode();
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
            var logMsg = Arrays.stream(content.get(i).split("[\\s=:,]"))
                    .filter(x-> !x.isEmpty())
                    .toList();
            var constLogMsg = logMsg.stream().filter(x -> !x.equals("<*>")).toList();

            var matchCluster = prefixTreeMatch(prefixTreeRoot, constLogMsg, 0);
            if (matchCluster == null){
                matchCluster = simpleLoopMatch(this.cluster, constLogMsg);
                if (matchCluster == null){
                    matchCluster = LCSMatch(this.cluster, logMsg);
                    if (matchCluster == null){
                        var ids = new ArrayList<Integer>();
                        ids.add(logID);
                        var newCluster = new LogCluster(logMsg,  ids);
                        this.cluster.add(newCluster);
                        addSeqToPrefixTree(prefixTreeRoot, newCluster);
                    }else{
                        var newTemplate = getTemplate(LCS(logMsg, matchCluster.getTemplate()));
                        if (!newTemplate.equals(matchCluster.getTemplate())){
                            removeSeqFromPrefixTree(prefixTreeRoot, matchCluster);
                        }
                    }
                }
            }
            if (matchCluster != null){
                for (int j = 0; j < cluster.size(); j++) {
                    if (matchCluster.getTemplate().equals(cluster.get(j).getTemplate()))
                        cluster.get(j).ids.add(logID);
                    break;
                }
            }
            if (i % 100 == 0){
//                Get time to check 10 min interval for logging purposes
            }
            if (i % 10000 == 0 || i == content.size() ){
                logger.log(Level.INFO, String.format("Processed %.2f%% of log lines.", 100*i/content.size()));
            }
        }

    }

    private void removeSeqFromPrefixTree(TrieNode prefixTreeRoot, LogCluster newCluster) {
        var parentn = prefixTreeRoot;
        var seq = newCluster.getTemplate().stream().filter(x -> !x.equals("<*>")).toList();
        for (String tok : seq) {
            if (parentn.child.containsKey(tok)){
                var matched = parentn.child.get(tok);
                if (matched.templateNo == 1){
                    parentn.child.remove(tok);
                    break;
                }else {
                    matched.templateNo--;
                    parentn = matched;
                }
            }
        }
    }

    private List<String> getTemplate(List<String> lcs) {
        var res = new ArrayList<String>();
        if (lcs.isEmpty())
            return res;

        Collections.reverse(lcs);
        int i = 0;
        for (String tok : lcs) {
            i++;
            if (tok.equals(lcs.get(lcs.size() - 1))){
                res.add(tok);
                lcs.remove(lcs.size() - 1);
            }else
                res.add("<*>");
            if (lcs.isEmpty())
                break;
        }
        if (i < lcs.size())
            res.add("<*>");
        return res;
    }

    private void addSeqToPrefixTree(TrieNode prefixTreeRoot, LogCluster newCluster) {
        var parentn = prefixTreeRoot;
        var seq = newCluster.getTemplate().stream().filter(x -> !x.equals("<*>")).toList();
        for (String tok : seq) {
            if (parentn.child.containsKey(tok))
                parentn.child.get(tok).templateNo ++;
            else
                parentn.child.put(tok, new TrieNode(tok, 1));
            parentn = parentn.child.get(tok);
        }
        if (parentn == null)
            parentn.cluster = newCluster;
    }

    private LogCluster LCSMatch(List<LogCluster> cluster, List<String> logMsg) {
        LogCluster res = null;
        var msgSet = new HashSet<String>(logMsg);
        var msgLen = logMsg.size();
        var maxLen = -1;
        LogCluster maxLCS = null;

        for (LogCluster logCluster : cluster) {
            var tempSet = new HashSet<String>(logCluster.getTemplate());
            tempSet.retainAll(tempSet);
            if (tempSet.size() < .5 * msgLen)
                continue;
            var lcs = LCS(logMsg, logCluster.getTemplate());
            var lenLcs = lcs.size();
            if (lenLcs > maxLen ||
                    (lenLcs == maxLen &&
                            logCluster.getTemplate().size() < maxLCS.getTemplate().size())){
                maxLen = lenLcs;
                maxLCS = logCluster;
            }
        }

        if (maxLen >= tau * msgLen)
            res = maxLCS;

        return res;
    }

    private List<String> LCS(List<String> seq1, List<String> seq2) {
        int[][] lengths = new int[seq1.size()+1][seq2.size()+1];
        for (int i = 0; i < seq1.size(); i++) {
            for (int j = 0; j < seq2.size(); j++) {
                if (seq1.get(i).equals(seq2.get(j)))
                    lengths[i+1][j+1] = lengths[i][j]+1;
                else
                    lengths[i+1][j+1] = Integer.max(lengths[i+1][j], lengths[i][j+1]);
            }
        }
        var result = new ArrayList<String>();
        var lenOfSeq1= seq1.size();
        var lenOfSeq2 = seq2.size();
        while (lenOfSeq1 != 0 && lenOfSeq2 != 0){
            if (lengths[lenOfSeq1][lenOfSeq2] == lengths[lenOfSeq1-1][lenOfSeq2])
                lenOfSeq1--;
            else if (lengths[lenOfSeq1][lenOfSeq2] == lengths[lenOfSeq1][lenOfSeq2-1])
                lenOfSeq2--;
            else
                assert (seq1.get(lenOfSeq1-1).equals(seq2.get(lenOfSeq2-1)));
                result.add(0, seq1.get(lenOfSeq1-1));
                lenOfSeq1--;
                lenOfSeq2--;
        }
        return result;
    }

    private LogCluster simpleLoopMatch(List<LogCluster> cluster, List<String> constLogMsg) {
        for (LogCluster logCluster : cluster) {
            if (logCluster.getTemplate().size() < .5 * constLogMsg.size())
                continue;
            var tokenSet = new HashSet<String>(constLogMsg);
            if (constLogMsg.stream().allMatch(x -> x == "<*>" || tokenSet.contains(x)))
                return logCluster;
        }
        return null;
    }

    private LogCluster prefixTreeMatch(TrieNode prefixTree, List<String> constLogMsg, int start) {
        for (int i = start; i < constLogMsg.size(); i++) {
            if (prefixTree.child.containsKey(constLogMsg.get(i))){
                var child = prefixTree.child.get(constLogMsg.get(i));
                var tmp = child.cluster.getTemplate();
                if (tmp != null){
                    var constLM = tmp.stream().filter(x -> !x.equals("<*>")).toList();
                    if (constLM.size() >= tau * constLogMsg.size())
                        return child.cluster;
                }else
                    prefixTreeMatch(child, constLogMsg, i+1);
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
    List<String> template;

    List<Integer> ids;

    public LogCluster(List<String> template, List<Integer> ids) {
        this.template = template;
        this.ids = ids;
    }

    public List<String> getTemplate() {
        return template;
    }

    public void setTemplate(List<String> template) {
        this.template = template;
    }

    public List<Integer> getIds() {
        return ids;
    }

    public void setIds(List<Integer> ids) {
        this.ids = ids;
    }
}

class TrieNode {
    LogCluster cluster;
    String token;
    int templateNo;
    Map<String, TrieNode> child;

    public TrieNode() {
        this.child = new HashMap<>();
    }

    public TrieNode(String token, int templateNo) {
        this.token = token;
        this.templateNo = templateNo;
        this.child = new HashMap<>();
    }
}