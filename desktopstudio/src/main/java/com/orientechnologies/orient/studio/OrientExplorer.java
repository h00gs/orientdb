/*
 * Copyright 2013, Geomatys
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.studio;

import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.graph.OGraphDatabasePool;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import javax.swing.*;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableModel;
import javax.swing.tree.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.*;
import java.util.List;

/**
 * Administration tool for OrientDB.
 * 
 * @author Johann Sorel (Geomatys)
 */
public class OrientExplorer extends JFrame {
    
    private static final Icon ICON_CLUSTER = UIManager.getIcon("FileChooser.listViewIcon");
    private static final Icon ICON_CONNECTED = UIManager.getIcon("FileView.computerIcon");
    private static final Icon ICON_DISCONNECTED = UIManager.getIcon("FileView.computerIcon");
    private static final Icon ICON_DOCUMENT = UIManager.getIcon("FileView.directoryIcon");
    private static final Icon ICON_FIELD = UIManager.getIcon("FileView.fileIcon");
    
    private static final Comparator<OClass> CLASS_COMPARATOR = new Comparator<OClass>() {

        @Override
        public int compare(OClass o1, OClass o2) {
            return o1.getName().compareTo(o2.getName());
        }
    };
    
    private static final Comparator<OProperty> PROPERTY_COMPARATOR = new Comparator<OProperty>() {

        @Override
        public int compare(OProperty o1, OProperty o2) {
            return o1.getName().compareTo(o2.getName());
        }
    };
    
    private final JPanel guiConfigPane = new JPanel(new GridLayout(3, 3));
    private final JTextField guiURL = new JTextField();
    private final JTextField guiUser = new JTextField();
    private final JTextField guiPassword = new JTextField();
    
    private final JToolBar guiToolbar = new JToolBar();
        
    private final JPanel guiObjConfig = new JPanel(new BorderLayout());    
    private final DefaultMutableTreeNode root = new DefaultMutableTreeNode();
    private final DefaultTreeModel treeModel = new DefaultTreeModel(root);
    private final JTree guiTree = new OrientTree(treeModel);
        
    public OrientExplorer(){
        
        guiConfigPane.add(new JLabel("URL"));
        guiConfigPane.add(guiURL);
        guiConfigPane.add(new JLabel("User"));
        guiConfigPane.add(guiUser);
        guiConfigPane.add(new JLabel("Password"));
        guiConfigPane.add(guiPassword);
        
        guiTree.setCellRenderer(new OrientCellRenderer());
        guiTree.setModel(treeModel);
        
        final JMenuBar menubar = new JMenuBar();
        final JMenu menu = new JMenu("File");
        menubar.add(menu);
        final JMenuItem item = new JMenuItem("Exit");
        item.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                System.exit(0);
            }
        });
        menu.add(item);
        setJMenuBar(menubar);
        
        guiToolbar.setFloatable(false);
        guiToolbar.add(new AbstractAction("New connection", ICON_DISCONNECTED) {
            @Override
            public void actionPerformed(ActionEvent e) {
                JOptionPane.showMessageDialog(null,guiConfigPane);
                final ConnectionRef cnxref = new ConnectionRef(guiURL.getText(), guiUser.getText(), guiPassword.getText());
                updateModel(cnxref);
            }
        });
        
        final JSplitPane container = new JSplitPane();
        container.setDividerSize(3);
        container.setLeftComponent(new JScrollPane(guiTree));
        container.setRightComponent(guiObjConfig);
        container.setDividerLocation(300);
        
        final JPanel all = new JPanel(new BorderLayout());
        all.add(BorderLayout.CENTER,container);
        all.add(BorderLayout.NORTH,guiToolbar);
        
        setContentPane(all);
    }
    
    private void updateModel(ConnectionRef ref){
                
        final DefaultMutableTreeNode cnxroot = new DefaultMutableTreeNode(ref);
        
        OGraphDatabase cnx = null;
        try {
            cnx = ref.getConnection();
            final OMetadata metadata = cnx.getMetadata();
            final OSchema schema = metadata.getSchema();

            final List<OClass> classes = new ArrayList<OClass>(schema.getClasses());
            Collections.sort(classes, CLASS_COMPARATOR);
            for (OClass clazz : classes) {
                final DefaultMutableTreeNode node = new DefaultMutableTreeNode(clazz);
                final List<OProperty> props = new ArrayList<OProperty>(clazz.properties());
                Collections.sort(props, PROPERTY_COMPARATOR);
                for (OProperty prop : props) {
                    final DefaultMutableTreeNode pnode = new DefaultMutableTreeNode(prop);
                    node.add(pnode);
                }
                cnxroot.add(node);
                
                final int[] clusterIds = clazz.getClusterIds();
                for(int cid : clusterIds){
                    final String cname = cnx.getClusterNameById(cid);
                    final ClusterRef cref = new ClusterRef(cid, cname);
                    final DefaultMutableTreeNode cnode = new DefaultMutableTreeNode(cref);
                    node.add(cnode);
                }
                
            }

        } finally {
            if (cnx != null) {
                cnx.close();
            }
        }

        root.add(cnxroot);
        treeModel.reload();
    }
    
    private class OrientTree extends JTree{

        public OrientTree(TreeModel newModel) {
            super(newModel);
        }

        @Override
        public JPopupMenu getComponentPopupMenu() {
            final TreePath[] paths = getSelectionPaths();
            if(paths == null || paths.length > 1) return null;
            
            final DefaultMutableTreeNode node = (DefaultMutableTreeNode) paths[0].getLastPathComponent();
            final Object candidate = node.getUserObject();
            
            final JPopupMenu menu = new JPopupMenu();
            
            if(candidate instanceof OClass){
                final OClass clazz = (OClass) candidate;
                final ConnectionRef cnxref = (ConnectionRef) ((DefaultMutableTreeNode)node.getParent()).getUserObject();
                final JMenuItem item = new JMenuItem(new AbstractAction("Show") {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        final SearchPane searchtab = new SearchPane(cnxref);
                        searchtab.guiQuery.setText("SELECT FROM "+clazz.getName());
                        searchtab.updateData();
                        searchtab.setVisible(true);
                    }
                });
                menu.add(item);
            }
            
            return menu;
        }
                
    }
    
    private static class OrientCellRenderer extends DefaultTreeCellRenderer {

        @Override
        public Component getTreeCellRendererComponent(JTree tree, Object value, boolean sel, boolean expanded, boolean leaf, int row, boolean hasFocus) {
            final JLabel lbl = (JLabel) super.getTreeCellRendererComponent(tree, value, sel, expanded, leaf, row, hasFocus);
            
            if(value instanceof DefaultMutableTreeNode){
                value = ((DefaultMutableTreeNode)value).getUserObject();
            }
            
            lbl.setIcon(null);
            
            if(value instanceof ConnectionRef){
                final ConnectionRef candidate = (ConnectionRef) value;
                lbl.setText(candidate.url +" ("+candidate.user+"/*****)");
                lbl.setIcon(ICON_CONNECTED);
            }else if(value instanceof OClass){
                final OClass candidate = (OClass) value;
                lbl.setText(candidate.getName());
                lbl.setIcon(ICON_DOCUMENT);
            }else if(value instanceof ClusterRef){
                final ClusterRef candidate = (ClusterRef) value;
                lbl.setText("CLUSTER : "+candidate.name);
                lbl.setIcon(ICON_CLUSTER);
            }else if(value instanceof OProperty){
                final OProperty property = (OProperty) value;
                
                final OType basetype = property.getType();
                final OType linktype = property.getLinkedType();
                final OClass linkclass = property.getLinkedClass();

                
                String name = property.getName();

                if(basetype == OType.EMBEDDED){
                    name = "[E] " + name;                
                }else if(basetype == OType.EMBEDDEDLIST){
                    name = "[EL] " + name;
                }else if(basetype == OType.EMBEDDEDMAP){
                    name = "[EM] " + name;
                }else if(basetype == OType.EMBEDDEDSET){
                    name = "[ES] " + name;
                }else if(basetype == OType.LINK){
                    name = "[L] " + name;
                }else if(basetype == OType.LINKLIST){
                    name = "[LL] " + name;
                }else if(basetype == OType.LINKMAP){
                    name = "[LM] " + name;
                }else if(basetype == OType.LINKSET){
                    name = "[LS] " + name;
                }else{
                    name += " ("+basetype.name()+")";
                }

                if(linktype != null){
                    name += " ("+linktype.name()+")";
                }

                if(linkclass != null){
                    name += " ("+linkclass.getName()+")";
                }

                final Collection<OIndex<?>> indexes = property.getAllIndexes();
                if(indexes != null && !indexes.isEmpty()){
                    name += " {Indexed}";
                }        
                
                lbl.setText(name);
                lbl.setIcon(ICON_FIELD);
            }
            
            return lbl;
        }
        
    }
    
    private static class DocTableModel extends AbstractTableModel{

        private final List<ODocument> docs;
        private final List<String> names = new ArrayList<String>();

        public DocTableModel(List<ODocument> docs) {
            this.docs = docs;
            
            final Set<String> cn = new HashSet<String>();
            
            for(ODocument doc : docs){
                for(String name : doc.fieldNames()){
                    cn.add(name);
                }
            }
            names.addAll(cn);
            names.add(0,"orid");
            
        }
        
        @Override
        public int getRowCount() {
            return docs.size();
        }

        @Override
        public String getColumnName(int columnIndex) {
            return names.get(columnIndex);
        }

        @Override
        public int getColumnCount() {
            return names.size();
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            
            if(columnIndex == 0){
                return docs.get(rowIndex).getIdentity();
            }else{
                return docs.get(rowIndex).field(names.get(columnIndex));
            }
        }
        
    }
    
    private static class ConnectionRef{
        
        private OGraphDatabasePool pool;
        private String url;
        private String user;
        private String password;

        public ConnectionRef(String url, String user, String password) {
            this.url = url;
            this.user = user;
            this.password = password;
        }

        public synchronized OGraphDatabasePool getPool() {
            if(pool == null){
                pool = new OGraphDatabasePool(url,user,password);
            }
            return pool;
        }
        
        public OGraphDatabase getConnection(){
            OGraphDatabase cnx = getPool().acquire();
            cnx.reload();
            cnx.getLevel2Cache().setEnable(false);
            cnx.getLevel1Cache().setEnable(false);
            return cnx;
        }
    
    }
    
    private static class ClusterRef{
        
        private final int id;
        private final String name;

        public ClusterRef(int id, String name) {
            this.id = id;
            this.name = name;
        }
        
    }
    
    private static class SearchPane extends JFrame{
        
        private final JSQLTextPane guiQuery = new JSQLTextPane();
        private final JButton guiExecute = new JButton("Execute");
        private final JTable guiTable = new JTable();
        private final JScrollPane guiResult = new JScrollPane(guiTable);
        private final ConnectionRef ref;
        
        public SearchPane(ConnectionRef ref){
            super(ref.url);
            setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
            this.ref = ref;
            
            guiExecute.addActionListener(new ActionListener() {
                @Override
                public void actionPerformed(ActionEvent e) {
                    updateData();
                }
            });

            final JPanel north = new JPanel(new BorderLayout());
            final JPanel northAct = new JPanel(new FlowLayout(FlowLayout.RIGHT));
            north.add(BorderLayout.CENTER, new JScrollPane(guiQuery));
            north.add(BorderLayout.SOUTH, northAct);
            northAct.add(guiExecute);

            final JSplitPane center = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
            center.setDividerSize(3);
            center.setTopComponent(north);
            center.setBottomComponent(guiResult);
            center.setDividerLocation(150);
            
            setContentPane(new JPanel(new BorderLayout()));
            add(BorderLayout.CENTER,center);
            pack();
            setLocationRelativeTo(null);
        }
        
        private void updateData() {
            guiTable.setModel(new DefaultTableModel());

            if (ref.getPool() == null) {
                return;
            }

            final String query = guiQuery.getText().trim();
            if (query.isEmpty()) {
                return;
            }

            OGraphDatabase cnx = null;
            try {
                cnx = ref.getConnection();

                final OSQLSynchQuery sq = new OSQLSynchQuery(query);
                final List<ODocument> docs = cnx.query(sq);
                guiTable.setModel(new DocTableModel(docs));

            } finally {
                if (cnx != null) {
                    cnx.close();
                }
            }

        }

        
    }
    
    public static void main(String[] args) throws UnsupportedLookAndFeelException {
                
        final OrientExplorer exp = new OrientExplorer();
        exp.setSize(1024, 768);
        exp.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        exp.setLocationRelativeTo(null);
        exp.setVisible(true);
        
    }
    
}
