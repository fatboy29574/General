/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package inventoryexplode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author AReaves
 */
public class InventoryExplode {

    /**
     * @param args the command line arguments
     */
    Connection msCon = null;
    Queue<Case> caseq;
    Queue<MealBag> mealBagq;
    Queue<SubAssembly> subAssemblyq;
    TreeMap<String, GFMComponent> gfmComponentm;
    PrintWriter pw;
    Statement msState;

    public InventoryExplode() {
        caseq = new LinkedList<>();
        mealBagq = new LinkedList<>();
        subAssemblyq = new LinkedList<>();
        gfmComponentm = new TreeMap<>();

        String msUrl = "jdbc:sqlserver://10.15.10.194;instancename=ROSSERP;DatabaseName=fin_prod";

        try {
            msCon = DriverManager.getConnection(msUrl, "fin_prod", "fin_prod");
            msState = msCon.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        } catch (SQLException sq) {
            System.out.println(sq.toString());
            System.exit(1);
        }
        try {
            pw = new PrintWriter("Exploded.txt");
        } catch (FileNotFoundException ex) {
            Logger.getLogger(InventoryExplode.class.getName()).log(Level.SEVERE, null, ex);
        }
        loadCase();
        while (caseq.peek() != null) {
            Case nextCase = caseq.poll();
            nextCase.explodeCase(); 
        }
        loadMealBag();
        while (mealBagq.peek() != null) {
            MealBag meally = mealBagq.poll();
            meally.explodeMB();
        }
        /*       loadSubAssembly(); */
        while (subAssemblyq.peek() != null) {
            SubAssembly subs = subAssemblyq.poll();
            subs.explodeSub();
        }
        /*       loadGFM(); */
        dumpGFM();

        pw.close(); // close the PrintWriter
    }

    public static void main(String[] args) {
        new InventoryExplode();

    }

    private void loadCase() {

        /*  String caseQuery = "SELECT IL.PART_CODE,PM.PART_DESC_1,IL.IC_LOT_NUMBER,IL.IC_QUANTITY FROM "
         + "fin_prod.dbo.IC_LOT_STATUS IL JOIN fin_prod.dbo.PRODUCT_MASTER PM ON IL.COMPANY_CODE = PM.COMPANY_CODE AND IL.PART_CODE = PM.PART_CODE"
         + " WHERE IL.PART_CODE = '920000' AND IL.IC_QUANTITY > 0 AND IL.WAREHOUSE ='M1'" ;
         try {
         msState = msCon.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
         ResultSet rs = msState.executeQuery(caseQuery);
         while (rs.next()) {
         Case newCase = new Case();
         newCase.part = rs.getString("PART_CODE");
         newCase.desc = rs.getString("PART_DESC_1");
         newCase.lot = rs.getString("IC_LOT_NUMBER");
         double qty = rs.getDouble("IC_QUANTITY");
         newCase.qty = (long) qty;
         caseq.add(newCase);

         pw.println(newCase.part + "," + newCase.desc + "," + newCase.lot + "," + newCase.qty);
         pw.flush();
         }
         rs.close();
         } catch (SQLException ex) {
         ex.printStackTrace();
         } */
        Case newCase = new Case();
        newCase.part = "920000";
        newCase.desc = "MRE Cases";
        newCase.lot = "6208";
        newCase.qty = 4736L;
        caseq.add(newCase);

    }

    private void loadMealBag() {
        // load from file
        FileReader reader = null;
        try {
            reader = new FileReader("MealBag.csv");

            BufferedReader buffy = new BufferedReader(reader);
            String liner;
            while ((liner = buffy.readLine()) != null) {
                System.out.println(liner);
                String[] splitTail;
                splitTail = liner.split(",");
                if (splitTail.length > 1) {
                    System.out.println(splitTail[7]);
                    
                     MealBag mealBag = new MealBag();
                     mealBag.part = splitTail[4];
                     mealBag.desc = splitTail[5];
                     mealBag.lot = splitTail[6];
                     mealBag.qty = (long) Double.parseDouble(splitTail[7]);
                     mealBagq.add(mealBag);
                     System.out.println(mealBag.part + " " + mealBag.desc + " " + mealBag.lot + " " + mealBag.qty);
                     
                }
            }
        } catch (FileNotFoundException fne) {
            System.out.println("Oh hell, no file !");
        } catch (IOException e) {

        }
        /*
         String mbQuery = " Select LS.PART_CODE,LS.IC_LOT_NUMBER,PM.PART_DESC_1,LS.IC_QUANTITY FROM "
         + " IC_LOT_STATUS LS JOIN PRODUCT_MASTER PM ON LS.COMPANY_CODE = PM.COMPANY_CODE AND "
         + " LS.PART_CODE = PM.PART_CODE "
         + " WHERE PM.PRODUCT_TYPE = '91' AND LS.IC_QUANTITY > 0 AND PM.PRODUCT_GROUP = 'MRE' "
         + " AND LS.WAREHOUSE <>'MC' ";
         try {
         msState = msCon.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
         ResultSet rs = msState.executeQuery(mbQuery);
         while (rs.next()) {
         MealBag mealBag = new MealBag();
         mealBag.part = rs.getString("PART_CODE");
         mealBag.desc = rs.getString("PART_DESC_1");
         mealBag.lot = rs.getString("IC_LOT_NUMBER");
         double qty = rs.getDouble("IC_QUANTITY");
         mealBag.qty = (long) qty;
         mealBagq.add(mealBag);
            
         pw.println(mealBag.part + "," + mealBag.desc + "," + mealBag.lot + "," + mealBag.qty);
         pw.flush();
         }
         rs.close();
         } catch (SQLException ex) {
         ex.printStackTrace();
         } */
    }

    private void loadSubAssembly() {
        String mbQuery = " Select LS.PART_CODE,LS.IC_LOT_NUMBER,PM.PART_DESC_1,LS.IC_QUANTITY FROM "
                + " IC_LOT_STATUS LS JOIN PRODUCT_MASTER PM ON LS.COMPANY_CODE = PM.COMPANY_CODE AND "
                + " LS.PART_CODE = PM.PART_CODE "
                + " WHERE PM.PRODUCT_TYPE = '90' AND LS.IC_QUANTITY > 0 AND PM.PRODUCT_GROUP = 'MRE' "
                + " AND LS.WAREHOUSE <> 'MC' ";
        try {
            msState = msCon.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = msState.executeQuery(mbQuery);
            while (rs.next()) {
                SubAssembly sub = new SubAssembly();
                sub.part = rs.getString("PART_CODE");
                sub.desc = rs.getString("PART_DESC_1");
                sub.lot = rs.getString("IC_LOT_NUMBER");
                double qty = rs.getDouble("IC_QUANTITY");
                sub.qty = (long) qty;
                subAssemblyq.add(sub);

                pw.println(sub.part + "," + sub.desc + "," + sub.lot + "," + sub.qty);
                pw.flush();
            }
            rs.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    private void dumpGFM() {
        PrintWriter printG = null;

        try {
            printG = new PrintWriter("GFMInventory.txt");
        } catch (FileNotFoundException ex) {
            Logger.getLogger(InventoryExplode.class.getName()).log(Level.SEVERE, null, ex);
        }
        for (Map.Entry<String, GFMComponent> entry : gfmComponentm.entrySet()) {
            String eanNumber = "999999999999";
            if (entry.getValue().NIIN != null && entry.getValue().NIIN.length() > 5) {
                eanNumber = entry.getValue().NIIN;
            }
            printG.println("RS22" + "\t" + eanNumber.substring(0, 4) + "\t\"" + eanNumber.substring(4) + "\"\t" + "A" + "\t" + " " + "\t"
                    + entry.getValue().qty + "\t" + "EAC" + "\t" + entry.getValue().desc + "\t" + "00000" + "\t" + "00000" + "\t" + entry.getValue().part);
            printG.flush();
        }
        printG.close();
    }

    private void loadGFM() {
        String mbQuery = " Select LS.PART_CODE,LS.IC_LOT_NUMBER,PM.PART_DESC_1,PM.EAN_NUMBER,PM.PRODUCT_GROUP,LS.IC_QUANTITY FROM "
                + " IC_LOT_STATUS LS JOIN PRODUCT_MASTER PM ON LS.COMPANY_CODE = PM.COMPANY_CODE AND "
                + " LS.PART_CODE = PM.PART_CODE "
                + " WHERE PM.PRODUCT_GROUP = 'GFM' AND LS.IC_QUANTITY > 0  "
                + " AND LS.WAREHOUSE <> 'MC' ";
        try {
            msState = msCon.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = msState.executeQuery(mbQuery);
            while (rs.next()) {
                String partCodeI = rs.getString("PART_CODE");
                String partDesc = rs.getString("PART_DESC_1");
                String eanNumber = rs.getString("EAN_NUMBER");
                String prodGroup = rs.getString("PRODUCT_GROUP");

                String lotNumber = rs.getString("IC_LOT_NUMBER");
                double adjqty = rs.getDouble("IC_QUANTITY");

                if (gfmComponentm.containsKey(partCodeI)) {
                    GFMComponent gfmc = gfmComponentm.get(partCodeI);
                    gfmc.qty = (long) (gfmc.qty + adjqty);
                    gfmComponentm.put(partCodeI, gfmc);
                    pw.println("GFMPART cumulative" + gfmc.part + "," + partDesc + "," + gfmc.qty);
                    pw.flush();
                } else {

                    GFMComponent gfmc = new GFMComponent();
                    gfmc.part = partCodeI;
                    gfmc.desc = partDesc;
                    gfmc.NIIN = eanNumber;
                    gfmc.qty = (long) rs.getDouble("IC_QUANTITY");
                    gfmComponentm.put(gfmc.part, gfmc);
                    pw.println("GFMPART " + gfmc.part + "," + gfmc.desc + "," + lotNumber + "," + gfmc.qty);
                    pw.flush();

                }

            }
            rs.close();

            msState.close();

        } catch (SQLException ex) {
            ex.printStackTrace();

        }
    }

    class Case {

        String part;
        String desc;
        String lot;
        long qty;

        public Case() {
        }

        void explodeCase() {
            String boom = "SELECT JO.JOB_NUMBER,JO.TOTAL_CLOSE_QTY,JI.PART_CODE,JI.PART_DESC_1,JI.EAN_NUMBER,JI.PRODUCT_GROUP,JI.PRODUCT_TYPE,"
                    + "JI.IC_LOT_NUMBER,JI.OUTSTANDING_QTY  FROM "
                    + "(SELECT IM.COMPANY_CODE,MAX(IM.DOCUMENT_NUMBER) AS DOCUMENT_NUMBER,IM.FACTORY "
                    + "FROM fin_prod.dbo.IC_MOVEMENTS IM "
                    + "WHERE IM.PART_CODE = " + "'" + this.part + "'"
                    + "AND IM.IC_LOT_NUMBER =" + "'" + this.lot + "'"
                    + "AND IM.IC_QUANTITY_FIELD = 'IC_QUANTITY' "
                    + "AND IM.MOVEMENT_CODE = 'PMMC' "
                    + "GROUP BY IM.COMPANY_CODE,IM.FACTORY "
                    + " HAVING SUM(IC_MOVE_QUANTITY) > 0) IM "
                    + "JOIN "
                    + "(SELECT COMPANY_CODE,JOB_NUMBER,FACTORY,PART_CODE,TOTAL_CLOSE_QTY "
                    + "FROM man_prod.dbo.JOB_OUTPUTS "
                    + "WHERE TOTAL_CLOSE_QTY >0) JO "
                    + "ON IM.COMPANY_CODE = JO.COMPANY_CODE AND IM.FACTORY = JO.FACTORY AND IM.DOCUMENT_NUMBER = JO.JOB_NUMBER "
                    + "JOIN "
                    + "(SELECT JM.COMPANY_CODE,JOB_NUMBER,JM.FACTORY,JM.PART_CODE,PM.PART_DESC_1,PM.EAN_NUMBER,JM.IC_LOT_NUMBER,JM.OUTSTANDING_QTY, "
                    + "PM.PRODUCT_TYPE,PM.PRODUCT_GROUP "
                    + "FROM man_prod.dbo.JOB_STAGE_LINE_MATERIAL JM JOIN fin_prod.dbo.PRODUCT_MASTER PM ON JM.COMPANY_CODE = PM.COMPANY_CODE "
                    + "AND JM.PART_CODE = PM.PART_CODE) JI "
                    + "ON JI.COMPANY_CODE = JO.COMPANY_CODE AND JI.FACTORY = JO.FACTORY AND JI.JOB_NUMBER = JO.JOB_NUMBER "
                    + "ORDER BY PART_CODE,IC_LOT_NUMBER ";

            try {
                msState = msCon.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                ResultSet rs = msState.executeQuery(boom);
                while (rs.next()) {

                    String job = rs.getString("JOB_NUMBER");
                    double totalCloseQty = rs.getDouble("TOTAL_CLOSE_QTY");
                    String partCodeI = rs.getString("PART_CODE");
                    String partDesc = rs.getString("PART_DESC_1");
                    String eanNumber = rs.getString("EAN_NUMBER");
                    String prodGroup = rs.getString("PRODUCT_GROUP");
                    String prodType = rs.getString("PRODUCT_TYPE");
                    String lotNumber = rs.getString("IC_LOT_NUMBER");
                    double issue = rs.getDouble("OUTSTANDING_QTY");
                    long adjqty = (long) (this.qty / totalCloseQty * issue + .5);
                    if (prodType.equals("91")) { // meal bag baby
                        MealBag mb = new MealBag();
                        mb.part = partCodeI;
                        mb.lot = lotNumber;
                        mb.qty = adjqty;
                        mealBagq.add(mb);
                        pw.println("explodeCase " + job + "," + totalCloseQty + "," + mb.part + "," + partDesc + "," + mb.lot + "," + mb.qty + ","
                                + this.part + "," + this.desc + "," + this.lot + "," + this.qty);
                        pw.flush();
                    } else if (prodGroup.equals("GFM")) {
                        if (gfmComponentm.containsKey(partCodeI)) {
                            GFMComponent gfmc = gfmComponentm.get(partCodeI);
                            gfmc.qty = gfmc.qty + adjqty;
                            gfmComponentm.put(partCodeI, gfmc);
                            pw.println("explodeCase: GFM" + job + "," + totalCloseQty + "," + gfmc.part + "," + partDesc + "," + gfmc.qty);
                            pw.flush();
                        } else {
                            GFMComponent gfmc = new GFMComponent();
                            gfmc.part = partCodeI;
                            gfmc.desc = partDesc;
                            gfmc.NIIN = eanNumber;
                            gfmc.qty = adjqty;
                            gfmComponentm.put(partCodeI, gfmc);
                            pw.println("explodeCase: GFMPART " + job + "," + totalCloseQty + "," + gfmc.part + "," + partDesc + "," + gfmc.qty);
                            pw.flush();
                        }

                    }

                }
                rs.close();
                msState.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }

        }
    }

    class MealBag {

        String part;
        String desc;
        String lot;
        long qty;

        public MealBag() {

        }

        void explodeMB() {
            String boom = "SELECT JO.JOB_NUMBER,JO.TOTAL_CLOSE_QTY,JI.PART_CODE,JI.PART_DESC_1,JI.EAN_NUMBER,JI.PRODUCT_GROUP,JI.PRODUCT_TYPE,"
                    + "JI.IC_LOT_NUMBER,JI.OUTSTANDING_QTY  FROM "
                    + "(SELECT IM.COMPANY_CODE,MAX(IM.DOCUMENT_NUMBER) AS DOCUMENT_NUMBER,IM.FACTORY "
                    + "FROM fin_prod.dbo.IC_MOVEMENTS IM "
                    + "WHERE IM.PART_CODE = " + "'" + this.part + "'"
                    + "AND IM.IC_LOT_NUMBER =" + "'" + this.lot + "'"
                    + "AND IM.IC_QUANTITY_FIELD = 'IC_QUANTITY' "
                    + "AND IM.MOVEMENT_CODE IN ('PMMC','PMMR') "
                    + "GROUP BY IM.COMPANY_CODE,IM.FACTORY  "
                    + " HAVING SUM(IC_MOVE_QUANTITY) > 0) IM "
                    + "JOIN "
                    + "(SELECT COMPANY_CODE,JOB_NUMBER,FACTORY,PART_CODE,TOTAL_CLOSE_QTY "
                    + "FROM man_prod.dbo.JOB_OUTPUTS "
                    + "WHERE TOTAL_CLOSE_QTY >0) JO "
                    + "ON IM.COMPANY_CODE = JO.COMPANY_CODE AND IM.FACTORY = JO.FACTORY AND IM.DOCUMENT_NUMBER = JO.JOB_NUMBER "
                    + "JOIN "
                    + "(SELECT JM.COMPANY_CODE,JOB_NUMBER,JM.FACTORY,JM.PART_CODE,PM.PART_DESC_1,PM.EAN_NUMBER,JM.IC_LOT_NUMBER,JM.OUTSTANDING_QTY, "
                    + "PM.PRODUCT_TYPE,PM.PRODUCT_GROUP "
                    + "FROM man_prod.dbo.JOB_STAGE_LINE_MATERIAL JM JOIN fin_prod.dbo.PRODUCT_MASTER PM ON JM.COMPANY_CODE = PM.COMPANY_CODE "
                    + "AND JM.PART_CODE = PM.PART_CODE) JI "
                    + "ON JI.COMPANY_CODE = JO.COMPANY_CODE AND JI.FACTORY = JO.FACTORY AND JI.JOB_NUMBER = JO.JOB_NUMBER "
                    + "ORDER BY PART_CODE,IC_LOT_NUMBER ";

            try {
                msState = msCon.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                ResultSet rs = msState.executeQuery(boom);
                while (rs.next()) {

                    String job = rs.getString("JOB_NUMBER");
                    double totalCloseQty = rs.getDouble("TOTAL_CLOSE_QTY");
                    String partCodeI = rs.getString("PART_CODE");
                    String partDesc = rs.getString("PART_DESC_1");
                    String eanNumber = rs.getString("EAN_NUMBER");
                    String prodGroup = rs.getString("PRODUCT_GROUP");
                    String prodType = rs.getString("PRODUCT_TYPE");
                    String lotNumber = rs.getString("IC_LOT_NUMBER");
                    double issue = rs.getDouble("OUTSTANDING_QTY");
                    long adjqty = (long) (this.qty / totalCloseQty * issue + .5);
                    if (prodType.equals("90")) { // subAssembly dude
                        SubAssembly sb = new SubAssembly();
                        sb.part = partCodeI;
                        sb.lot = lotNumber;
                        sb.qty = adjqty;
                        subAssemblyq.add(sb);
                        pw.println("explodeMB: SUBASSEMBLY " + job + "," + totalCloseQty + "," + sb.part + "," + partDesc + "," + sb.lot + "," + sb.qty + "," + this.qty);
                        pw.flush();
                    } else if (prodGroup.equals("GFM")) {
                        if (gfmComponentm.containsKey(partCodeI)) {
                            GFMComponent gfmc = gfmComponentm.get(partCodeI);
                            gfmc.qty = gfmc.qty + adjqty;
                            gfmComponentm.put(partCodeI, gfmc);
                            pw.println("explodeMB:GFM" + job + "," + totalCloseQty + "," + gfmc.part + "," + partDesc + "," + gfmc.qty + "," + this.part + "," + this.desc + "," + this.lot + "," + this.qty);
                            pw.flush();
                        } else {
                            GFMComponent gfmc = new GFMComponent();
                            gfmc.part = partCodeI;
                            gfmc.desc = partDesc;
                            gfmc.NIIN = eanNumber;
                            gfmc.qty = adjqty;
                            gfmComponentm.put(partCodeI, gfmc);
                            pw.println("explodeMB:GFMPART" + job + "," + totalCloseQty + "," + gfmc.part + "," + partDesc + "," + gfmc.qty);
                            pw.flush();
                        }
                    }

                }
                rs.close();
                msState.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }

        }
    }

    class SubAssembly {

        String part;
        String desc;
        String lot;
        long qty;

        public SubAssembly() {

        }

        private void explodeSub() {
            String boom = "SELECT JO.JOB_NUMBER,JO.TOTAL_CLOSE_QTY,JI.PART_CODE,JI.PART_DESC_1,JI.EAN_NUMBER,JI.PRODUCT_GROUP,JI.PRODUCT_TYPE,"
                    + "JI.IC_LOT_NUMBER,JI.OUTSTANDING_QTY  FROM "
                    + "(SELECT IM.COMPANY_CODE,MAX(IM.DOCUMENT_NUMBER) AS DOCUMENT_NUMBER,IM.FACTORY "
                    + "FROM fin_prod.dbo.IC_MOVEMENTS IM "
                    + "WHERE IM.PART_CODE = " + "'" + this.part + "'"
                    + "AND IM.IC_LOT_NUMBER =" + "'" + this.lot + "'"
                    + "AND IM.IC_QUANTITY_FIELD = 'IC_QUANTITY' "
                    //                    + "AND IM.MOVEMENT_CODE IN ('PMMC','PMMR') "
                    + "AND IM.MOVEMENT_CODE IN ('PMMC','PMJC') "
                    + "GROUP BY IM.COMPANY_CODE,IM.DOCUMENT_NUMBER,IM.FACTORY  "
                    + " HAVING SUM(IC_MOVE_QUANTITY) > 0) IM "
                    + "JOIN "
                    + "(SELECT COMPANY_CODE,JOB_NUMBER,FACTORY,PART_CODE,TOTAL_CLOSE_QTY "
                    + "FROM man_prod.dbo.JOB_OUTPUTS "
                    + "WHERE TOTAL_CLOSE_QTY >0) JO "
                    + "ON IM.COMPANY_CODE = JO.COMPANY_CODE AND IM.FACTORY = JO.FACTORY AND IM.DOCUMENT_NUMBER = JO.JOB_NUMBER "
                    + "JOIN "
                    + "(SELECT JM.COMPANY_CODE,JOB_NUMBER,JM.FACTORY,JM.PART_CODE,PM.PART_DESC_1,PM.EAN_NUMBER,JM.IC_LOT_NUMBER,JM.OUTSTANDING_QTY, "
                    + "PM.PRODUCT_TYPE,PM.PRODUCT_GROUP "
                    + "FROM man_prod.dbo.JOB_STAGE_LINE_MATERIAL JM JOIN fin_prod.dbo.PRODUCT_MASTER PM ON JM.COMPANY_CODE = PM.COMPANY_CODE "
                    + "AND JM.PART_CODE = PM.PART_CODE) JI "
                    + "ON JI.COMPANY_CODE = JO.COMPANY_CODE AND JI.FACTORY = JO.FACTORY AND JI.JOB_NUMBER = JO.JOB_NUMBER "
                    + "ORDER BY PART_CODE,IC_LOT_NUMBER ";

            try {
                msState = msCon.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                ResultSet rs = msState.executeQuery(boom);
                while (rs.next()) {

                    String job = rs.getString("JOB_NUMBER");
                    double totalCloseQty = rs.getDouble("TOTAL_CLOSE_QTY");
                    String partCodeI = rs.getString("PART_CODE");
                    String partDesc = rs.getString("PART_DESC_1");
                    String eanNumber = rs.getString("EAN_NUMBER");
                    String prodGroup = rs.getString("PRODUCT_GROUP");
                    String prodType = rs.getString("PRODUCT_TYPE");
                    String lotNumber = rs.getString("IC_LOT_NUMBER");
                    double issue = rs.getDouble("OUTSTANDING_QTY");
                    long adjqty = (long) (this.qty / totalCloseQty * issue + .5);

                    if (prodGroup.equals("GFM")) {

                        if (gfmComponentm.containsKey(partCodeI)) {
                            GFMComponent gfmc = gfmComponentm.get(partCodeI);
                            gfmc.qty = gfmc.qty + adjqty;
                            gfmComponentm.put(partCodeI, gfmc);
                            pw.println("explodeSub" + job + "," + totalCloseQty + "," + gfmc.part + "," + partDesc + "," + adjqty + "," + this.part + "," + this.desc + "," + this.lot + "," + this.qty);
                            pw.flush();
                        } else {
                            GFMComponent gfmc = new GFMComponent();
                            gfmc.part = partCodeI;
                            gfmc.desc = partDesc;
                            gfmc.NIIN = eanNumber;
                            gfmc.qty = adjqty;
                            gfmComponentm.put(partCodeI, gfmc);
                            pw.println("explodeSub" + job + "," + totalCloseQty + "," + gfmc.part + "," + partDesc + "," + gfmc.qty);
                            pw.flush();
                        }
                    }
                }
                rs.close();
                msState.close();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }

        }

    }

    class GFMComponent {

        String part;
        String desc;
        String NIIN;
        long qty;
    }

}
