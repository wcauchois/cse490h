package debug;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.regex.*;

import main.AppUtil;
import main.Protocol;

@SuppressWarnings({"unchecked", "serial"}) // fuck you java! i know what i'm doing!!
public abstract class PacketDisplayer {
    public static final Class[] displayers = new Class[] {
        PaxosDisplayer.class,
        RIODisplayer.class,
    };
    
    private static PacketDisplayer[] displayerInstances;
    private static Map<Integer, PacketDisplayer> displayersByProtocol = new HashMap<Integer, PacketDisplayer>();
    private static Map<String, PacketDisplayer> displayersByName = new HashMap<String, PacketDisplayer>();
    private static Map<Integer, String> protocolToName = new HashMap<Integer, String>();
    
    private static Set<Integer> addrWhitelist = new TreeSet<Integer>();
    private static List<AddrPair> addrPairWhitelist = new ArrayList<AddrPair>();
    private static Set<String> protocolsWhitelist = new HashSet<String>();

    private static Map<Integer, String> displayColors = new HashMap<Integer, String>() {{
        // XXX: choose better colors lol?
        put(Protocol.PAXOS, AppUtil.COLOR_CYAN);
        put(Protocol.RIO, AppUtil.COLOR_GREEN);
    }};
    private static final String defaultColor = AppUtil.COLOR_BLUE;
    
    public static String getDisplayColor(int protocol) {
        String color = displayColors.get(protocol);
        if(color != null)
            return color;
        else
            return defaultColor;
    }
    public static boolean useColors() {
        return useColors;
    }
    
    private static class AddrPair {
        public int from;
        public int to;
        public AddrPair(int from, int to) {
            this.from = from;
            this.to = to;
        }
        @Override
        public boolean equals(Object obj) {
            if(obj instanceof AddrPair) {
                AddrPair other = (AddrPair)obj;
                return this.to == other.to && this.from == other.from;
            } else
                return false;
        }
    }
    protected String protocolName;
    public PacketDisplayer(int protocol, String name) {
        this.protocolName = name;
        displayersByProtocol.put(protocol, this);
        displayersByName.put(name, this);
        protocolToName.put(protocol, name);
    }
    public static PacketDisplayer getDisplayer(int protocol) {
        return displayersByProtocol.get(protocol);
    }
    public abstract String displayPacket(int source, int dest, byte[] message);
    protected String genPrologue(int source, int dest) {
        return "Packet: " + + source + "->" + dest + " protocol: " + this.protocolName.toUpperCase() + " contents: ";
    }
    public static boolean shouldDisplay(int source, int dest, int protocol, byte[] message) {
        boolean display = true;
        
        if(!addrWhitelist.isEmpty()) {
            if(!addrWhitelist.contains(source) || !addrWhitelist.contains(dest)) {
                // if the whitelist doesn't contain source OR dest, don't display
                display = false;
            }
        }
        
        if(!addrPairWhitelist.isEmpty()) {
            if(!addrPairWhitelist.contains(new AddrPair(source, dest)))
                display = false;
        }
        
        if(!protocolsWhitelist.isEmpty()) {
            String protocolName = protocolToName.get(protocol);
            if(protocolName == null)
                display = false; // definitely not on the whitelist
            else if(!protocolsWhitelist.contains(protocolName))
                display = false;
        }
        
        PacketDisplayer displayer = displayersByProtocol.get(protocol);
        if(displayer != null && display == true)
            display = displayer.shouldDisplay(source, dest, message);
        
        return display;
    }
    private static boolean initialized = false;
    private static Pattern sectionPattern, fromToAddrPattern;
    private static boolean useColors = false;
    public static void initialize() {
        if(!initialized) {
            displayerInstances = new PacketDisplayer[displayers.length];
            try {
                for(int i = 0; i < displayers.length; i++) {
                    Constructor defaultCons = displayers[i].getConstructors()[0];
                    displayerInstances[i] = (PacketDisplayer)defaultCons.newInstance();
                }
            } catch(Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
            
            sectionPattern = Pattern.compile("\\[(\\w+)\\]"); // [configSection]
            fromToAddrPattern = Pattern.compile("(\\d+)\\s*->\\s*(\\d+)"); // fromAddr -> toAddr
            
            loadConfig();
            System.out.println("loaded debug configuration from " + CONFIG_FILENAME);
            initialized = true;
        }
    }
    protected boolean onConfigLine(String line, int lineNo) {
        return true; // whatever
    }
    protected void onConfigStart() { }
    protected void onConfigEnd() { }
    protected boolean shouldDisplay(int source, int dest, byte[] message) {
        return true;
    }
    public static final String CONFIG_FILENAME = "debug.conf";
    private static void loadConfig() {
        Scanner sc = null;
        try {
            sc = new Scanner(new File(CONFIG_FILENAME));
        } catch(IOException e) {
            e.printStackTrace();
            return;
        }
        
        Set<String> builtinSections = new HashSet<String>();
        Collections.addAll(builtinSections, "addrs", "protocols", "color");
        
        Matcher m;
        String currentSection = "";
        PacketDisplayer configHandler = null;
        int lineNo = 0;
        while(sc.hasNext()) {
            String line = sc.nextLine().trim();
            lineNo++;
            
            if(line.startsWith("#") || line.isEmpty())
                continue; // ignore comments and blank lines
            
            m = sectionPattern.matcher(line);
            if(m.matches()) {
                // we have a config section!!
                currentSection = m.group(1);
                if(configHandler != null)
                    configHandler.onConfigEnd();
                configHandler = displayersByName.get(currentSection);
                if(configHandler == null) {
                    if(!builtinSections.contains(currentSection)) {
                        System.err.println("unrecognized section in debug configuration: " + currentSection);
                    }
                } else
                    configHandler.onConfigStart();
            } else if(currentSection.equals("addrs")) {
                // we handle the [addrs] config section ourselves!!
                m = fromToAddrPattern.matcher(line);
                if(m.matches()) {
                    // we have a line like "from -> to"!
                    int from = Integer.parseInt(m.group(1)), to = Integer.parseInt(m.group(2));
                    addrPairWhitelist.add(new AddrPair(from, to));
                } else {
                    // the line should consist of a single integer...
                    addrWhitelist.add(Integer.parseInt(line.trim()));
                }
                // cool!
            } else if(currentSection.equals("protocols")) {
                // we also handle the [protocols] section
                if(displayersByName.containsKey(line)) {
                    protocolsWhitelist.add(line);
                } else {
                    System.err.println("unrecognized protocol " + line + "in debug configuration");
                }
            } else if(currentSection.equals("colors")) {
                if(line.equals("on"))
                    useColors = true;
                else
                    useColors = false;
            } else {
                // okay, this is a PacketDisplayer-specific config section... hand it off!
                if(configHandler != null) {
                    if(!configHandler.onConfigLine(line, lineNo))
                        System.err.println("bad parse on line #" + lineNo + "!");
                } else {
                    System.err.println("line #" + lineNo + " has data, but we're not in a section!");
                }
            }
        }
        if(configHandler != null)
            configHandler.onConfigEnd();
    }
}
