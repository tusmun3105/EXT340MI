    /**
   *  @Name: EXT340MI.LstSupplSummary
   *  @Description: Disponibilité, Prix et Délai
   *  @Authors: Tushaar Mungul
   */
  
  /**
   * CHANGELOGS
   * Version    Date    User        Description
   * 1.0.0      280824  KMOTEAN     Initial Release
   */
   
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.time.LocalDate

public class LstSupplSummary extends ExtendM3Transaction {
  private final MIAPI mi;
  private final DatabaseAPI database;
  private final ProgramAPI program;
  private final MICallerAPI miCaller;
  private String inCUNO;
  private String inITNO;
  private String inPOP4;
  private String inPOP5;
  private String inCFI1;
  private double inORQA;
  private String inWHLO;
  private String inROUT;
  private String inSPLM;
  private int inPADL;
  private int inBCKO;
  private int inOALT;
  private String ITNOfromMMS025="";
  private List<Map<String, String>> listresponseMMS025Global = []
  private List<Map<String, String>> listresponseCRS610_GetOrderInfoGlobal = []
  private List<Map<String, String>> listresponseMMS005_GetWarehouseGlobal = []
  private List<Map<String, String>> listresponseMMS200_GetSumWhsBalGlobal = []
  private List<Map<String, String>> listresponseMMS200_GetGlobal = []
  private List<Map<String, String>> listresponseDRS005_GetRouteGlobal = []
  private List<Map<String, String>> listresponseDRS006_LstGlobal = []
  private  def List<Map<String, String>> listresponseMMS059_ListGlobal = []
  private  def List<Map<String, String>> arrayofPlaceofLoading = []
  private  def List<Map<String, String>> arrayOFRoutes = []
  private  def List<Map<String, String>> arrayOFRoutesAllowforCuno = []
  private  def List<Map<String, String>> arrayOFDuration = []
  private  def List<Map<String, String>> arrayOFWarehouseQTY = []
  private List<Map<String, String>> listresponseDRS045_GetTIZODataGlobal = []
  private def uniqueData

  private def finalBigArray=[]
  private def updatedArray=[]
  private String timeZoneDateAndTime=""
  
  private String dateTimeZone="" 
  private String timeTimeZone="" 

  private List<String> distinctFWHLValues
  
  private String ORTP610=""
  private String WHLO610=""
  private String SPLM610=""
  private String PADL610=""
  private String BCKO610=""
  private String TOMU_MMS200=""
  private String PADL610_059=""
  private String BCKO610_059=""
  private String ITNOpop5or4=""
  private String AV01_MMS200=""
  private String WHNM_MMS200=""
  
  public LstSupplSummary(MIAPI mi, DatabaseAPI database, ProgramAPI program, MICallerAPI miCaller) {
    this.mi = mi;
    this.database = database;
    this.program = program;
    this.miCaller = miCaller;
  }
  
  public void main() {
     
     initializeInput();

     
    //get time timeZoneDateAndTime
     DRS045_ListApiCall(program.LDAZD.TIZO.toString())
          if(!listresponseDRS045_GetTIZODataGlobal.isEmpty()){
           dateTimeZone = listresponseDRS045_GetTIZODataGlobal[0].DATE.trim();
           timeTimeZone = listresponseDRS045_GetTIZODataGlobal[0].TIME.substring(0, 4).trim();
          }
          
     CRS610_GetOrderInfoApiCall(inCUNO);
     
     if(!listresponseCRS610_GetOrderInfoGlobal.isEmpty()){
          ORTP610 = listresponseCRS610_GetOrderInfoGlobal[0].ORTP;
          SPLM610 = listresponseCRS610_GetOrderInfoGlobal[0].SPLM;
          WHLO610 = listresponseCRS610_GetOrderInfoGlobal[0].WHLO;
          PADL610 = listresponseCRS610_GetOrderInfoGlobal[0].PADL;
          BCKO610 = listresponseCRS610_GetOrderInfoGlobal[0].BCKO;
          
     }else{
          mi.error("Customer "+inCUNO+" does not exist");
          return;
     }

   
  if(inITNO!=""){
    boolean checkItem = MMS200_GetApiCall(inITNO)
    if(checkItem!=true){
      mi.error("Item "+inITNO+ " does not exist")
      return
    }
  }
   
   
        checkBCKO_PADL_OALT_POP4_POP5();
     if (!isValidPOP5_PO4()) return
     
     if (!getItemNumber()) return

    
    if(inWHLO!=""){
      checkWarehouseExist(inWHLO)
     if (listresponseMMS005_GetWarehouseGlobal.isEmpty()){
       mi.error("Warehouse "+inWHLO.trim()+" does not exist")
       return
     }
    ifWHLOisnotEmpty(inWHLO.trim())
          }
    else if(inOALT==2){
    ifWHLOisnotEmpty(WHLO610)
      }
      else if(inSPLM.trim()==""){
    ifWHLOisnotEmpty(WHLO610)
      }
      else if(inOALT==1){
    ifWHLOisnotEmpty("")
      }
      else if(inOALT==0){
    ifWHLOisnotEmpty("")
      } 
      


     
  }
  
  void initializeInput(){
     inCUNO = mi.inData.get("CUNO") == null ? "" : mi.inData.get("CUNO").trim();
     inITNO = mi.inData.get("ITNO") == null ? "" : mi.inData.get("ITNO").trim();
     inPOP4 = mi.inData.get("POP4") == null ? "" : mi.inData.get("POP4").trim();
     inPOP5 = mi.inData.get("POP5") == null ? "" : mi.inData.get("POP5").trim();
     inCFI1 = mi.inData.get("CFI1") == null ? "" : mi.inData.get("CFI1").trim();
     inORQA = mi.in.get("ORQA") == null ? -1.0 as double: mi.in.get("ORQA") as Double;
     inWHLO = mi.inData.get("WHLO") == null ? "" : mi.inData.get("WHLO").trim();
     inROUT = mi.inData.get("ROUT") == null ? "" : mi.inData.get("ROUT").trim();
     inSPLM = mi.inData.get("SPLM") == null ? "" : mi.inData.get("SPLM").trim();
     inPADL = mi.in.get("PADL") == null ? -1 as Integer : mi.in.get("PADL") as Integer;
     inBCKO = mi.in.get("BCKO") == null ? -1 as Integer : mi.in.get("BCKO") as Integer;
     inOALT = mi.in.get("OALT") == null ? 0 as Integer : mi.in.get("OALT") as Integer;
  }
  
      void checkBCKO_PADL_OALT_POP4_POP5() {
        if (inPADL != 0 && inPADL != 1 && inPADL != -1) {
            mi.error("PADL can either be 0 or 1");
            return;
        } else if (inBCKO != 0 && inBCKO != 1 && inBCKO != -1) {
            mi.error("BCKO can either be 0 or 1");
            return;
        } else if (inOALT != 0 && inOALT != 1 && inOALT != 2 && inOALT != -1) {
            mi.error("OALT can either be 0, 1 or 2");
            return;
        } 
        // else if (inPOP4 == "" && inPOP5 == "") {
        //     mi.error("Either POP4 or POP5 needs to be assigned");
        //     return;
        // }
    }
    
      boolean isValidPOP5_PO4() {
      if(inPOP4.trim()!=""){
        MMS025ApiCall(inPOP4.trim(), "4");
        if(listresponseMMS025Global.isEmpty()){
          mi.error("Alias number "+inPOP4+" with Alias category 4 does not exist")
          return false
        }
      }
      if(inPOP5.trim()!=""){
        MMS025ApiCall(inPOP5.trim(), "5");
        if(listresponseMMS025Global.isEmpty()){
          mi.error("Alias number "+inPOP5+" with Alias category 5 does not exist")
          return false
        }
      }
      return true
    }
    
      void checkWarehouseExist(String whlo) {
            def paramsMMS005 = ["WHLO": "${whlo}".toString()]
    
            def callbackMMS005 = { Map<String, String> responseMMS005 ->
                if (responseMMS005!= null) {
                    if (responseMMS005.containsKey("error") && responseMMS005.error != null) {
                      
                    } else {
                        listresponseMMS005_GetWarehouseGlobal.add(responseMMS005)
                        
                    }
                }
            }
    
            miCaller.call("MMS005MI", "GetWarehouse", paramsMMS005, callbackMMS005)
            
    }   
    
    void CRS610_GetOrderInfoApiCall(String cuno) {
            def paramsCRS610 = ["CUNO": "${cuno}".toString()]
    
            def callbackCRS610 = { Map<String, String> responseCRS610 ->
                if (responseCRS610 != null) {
                    if (responseCRS610.containsKey("error") && responseCRS610.error != null) {
                    } else {
                        listresponseCRS610_GetOrderInfoGlobal.add(responseCRS610)
                    }
                }
            }
    
            miCaller.call("CRS610MI", "GetOrderInfo", paramsCRS610, callbackCRS610)
        }
        
    void MMS025ApiCall(String popn, String alwt) {
        
        def paramsMMS025 = ["POPN": "${popn}".toString(), "ALWT": "${alwt}".toString()]

        def callbackMMS025 = { Map<String, String> responseMMS025 ->
            if (responseMMS025 != null) {
                 if (responseMMS025.containsKey("error") && responseMMS025.error != null) {
                    } else {
                        listresponseMMS025Global.add(responseMMS025)
                    }
            }
        }
        miCaller.call("MMS025MI", "LstItem", paramsMMS025, callbackMMS025)
    }
    
    boolean getItemNumber(){
           if(inPOP4!=""){
             MMS025ApiCall(inPOP4, "4");
             if(listresponseMMS025Global.isEmpty()){
              mi.error("Alias Number "+inPOP4+" inexistant for Alias category "+"4");
              return false               
             }
           }
           else if(inPOP5!=""){
            MMS025ApiCall(inPOP5, "5");
             if(listresponseMMS025Global.isEmpty()){
              mi.error("Alias Number "+inPOP5+" inexistant for Alias category "+"5");
              return false              
             }            
           }
          if(!listresponseMMS025Global.isEmpty()){
               if(inITNO!=""){
                 ITNOpop5or4=inITNO
               }
               else{
               ITNOpop5or4=listresponseMMS025Global[0].ITNO;
               }
                return true
          }else{
            return true
          }
          
          
    }

    boolean MMS200_GetApiCall(String itno) {
            def paramsMMS200 = ["ITNO": "${itno}".toString()]
    
            def callbackMMS200 = { Map<String, String> responseMMS200 ->
                if (responseMMS200 != null) {
                    if (responseMMS200.containsKey("error") && responseMMS200.error != null) {
                    } else {
                        listresponseMMS200_GetGlobal.add(responseMMS200)
                    }
                }
            }
    
            miCaller.call("MMS200MI", "Get", paramsMMS200, callbackMMS200)
           if(listresponseMMS200_GetGlobal.size()>0){
             return true
           }
           else{
             return false
           }
        }
        
    // boolean MMS200_GetSumWhsBalApiCall(String whlo, String itno) {

    //         MMS200_GetItmWhsBasicApiCall(whlo, itno);

    //       if(!listresponseMMS200_GetItmWhsBasicGlobal.isEmpty()){
    //         TOMU_MMS200=listresponseMMS200_GetItmWhsBasicGlobal[0].TOMU;
    // }

    //         def paramsMMS200 = ["WHLO": "${whlo}".toString(), "POPN": "${itno}".toString()]
    
    //         def callbackMMS200 = { Map<String, String> responseMMS200 ->
    //             if (responseMMS200 != null) {
    //                 if (responseMMS200.containsKey("error") && responseMMS200.error != null) {
    //                 } else {
    //                     listresponseMMS200_GetSumWhsBalGlobal.add(responseMMS200)
    //                 }
    //             }
    //         }
            
    //         miCaller.call("MMS200MI", "GetSumWhsBal", paramsMMS200, callbackMMS200)

    //       if(!listresponseMMS200_GetSumWhsBalGlobal.isEmpty()){
    //           AV01_MMS200 = listresponseMMS200_GetSumWhsBalGlobal[0].AV01;
    //           WHNM_MMS200 = listresponseMMS200_GetSumWhsBalGlobal[0].WHNM;
    //           return true
    // }else{
    //   mi.error("Warehouse "+whlo+" does not exist");
    //   return false
    // }
    // }
    
    void MMS059_ListApiCall(String splm) {
            def paramsMMS059 = ["SPLM": "${splm}".toString()]
    
            def callbackMMS059 = { Map<String, String> responseMMS059 ->
                if (responseMMS059 != null) {
                    if (responseMMS059.containsKey("error") && responseMMS059.error != null) {
                    } else {
                        listresponseMMS059_ListGlobal.add(responseMMS059)
                    }
                }
            }
    
            miCaller.call("MMS059MI", "List", paramsMMS059, callbackMMS059)
        }
        
      void DRS045_ListApiCall(String tizo) {
            def paramsDRS045 = ["TIZO": "${tizo}".toString()]
    
            def callbackDRS045 = { Map<String, String> responseDRS045 ->
                if (responseDRS045 != null) {
                    if (responseDRS045.containsKey("error") && responseDRS045.error != null) {
                    } else {
                        listresponseDRS045_GetTIZODataGlobal.add(responseDRS045)
                    }
                }
            }
    
            miCaller.call("DRS045MI", "GetTIZOData", paramsDRS045, callbackDRS045)
        } 

    void DRS005_GetRouteApiCall(String rout) {
            def paramsDRS005 = ["ROUT": "${rout}".toString()]
    
            def callbackDRS005 = { Map<String, String> responseDRS005 ->
                if (responseDRS005 != null) {
                    if (responseDRS005.containsKey("error") && responseDRS005.error != null) {
                    } else {
                        listresponseDRS005_GetRouteGlobal.add(responseDRS005)
                    }
                }
            }
    
            miCaller.call("DRS005MI", "GetRoute", paramsDRS005, callbackDRS005)
        }
        
    void DRS006_LstApiCall(String rout) {
            def paramsDRS006 = ["ROUT": "${rout}".toString()]
    
            def callbackDRS006 = { Map<String, String> responseDRS006 ->
                if (responseDRS006 != null) {
                    if (responseDRS006.containsKey("error") && responseDRS006.error != null) {
                    } else {
                        listresponseDRS006_LstGlobal.add(responseDRS006)
                    }
                }
            }
    
            miCaller.call("DRS006MI", "Lst", paramsDRS006, callbackDRS006)
        }
        
    void retrievePlaceOfLoadingFromWarehouse(String whlo){

        int max_records = mi.getMaxRecords() <= 0 ? 100 : mi.getMaxRecords()
        ExpressionFactory expression = database.getExpressionFactory("MITWHL")

        if(listresponseMMS059_ListGlobal.size() > 1 && inWHLO.trim()=="" && inSPLM.trim()!=""){
        def whloValues = listresponseMMS059_ListGlobal*.FWHL.collect { it?.trim() }.findAll { it }
        def uniqueWhloValues = whloValues.unique()
        expression = expression.eq("MWWHLO", uniqueWhloValues[0])
        for (value in uniqueWhloValues) {
            expression = expression.or(expression.eq("MWWHLO", value.toString()))
        }


        }
        else if(listresponseMMS059_ListGlobal.size()>1 && (inOALT==0 || inOALT==1) && inWHLO.trim()==""){
        def whloValues = listresponseMMS059_ListGlobal*.FWHL.collect { it?.trim() }.findAll { it }.unique()

        def uniqueWhloValues = whloValues.unique()
        expression = expression.eq("MWWHLO", uniqueWhloValues[0])
        for (value in uniqueWhloValues) {
            expression = expression.or(expression.eq("MWWHLO", value.toString()))
        }          
        }
        else{
         expression = expression.eq("MWWHLO", whlo)
        }
        // whloValues.each { value ->
        //   expression= expression.or(expression.eq("MWWHLO", value))
        // }
        
        expression = expression.and(expression.eq("MWCONO", program.LDAZD.get("CONO") as String))

                DBAction query = database.table("MITWHL")
                .index("00")
                .matching(expression)
                .selectAllFields()
                .build()
        DBContainer container = query.getContainer()
        def myList = []
        query.readAll(container, 0, max_records) { DBContainer container1 ->
            def myObject = [MWCONO: container1.get("MWCONO").toString(), MWWHLO: container1.get("MWWHLO").toString(), MWSDES: container1.get("MWSDES").toString(), MWWHNM: container1.get("MWWHNM").toString()]
            myList.add(myObject)
        }
        arrayofPlaceofLoading=myList
        retrieveRouteFromPlaceofLoading()
        
    }
    
    
    
    
    
    
    
            void retrieveRouteFromPlaceofLoadingAndCustomer() {
            int max_records = mi.getMaxRecords() <= 0 ? 10000 : mi.getMaxRecords()
            ExpressionFactory expressionFactory = database.getExpressionFactory("DRODPR")
            def placeOfLoadingValues = arrayofPlaceofLoading.collect { it?.MWSDES?.trim() }.findAll { it }
            
            if (placeOfLoadingValues.isEmpty()) {
                return
            }
            def expression = expressionFactory.eq("DOEDES", placeOfLoadingValues[0]).and(expressionFactory.eq("DOOBV1", inCUNO.trim()))
            placeOfLoadingValues.drop(1).each { mwsdes ->
                expression = expression.or(expressionFactory.eq("DOEDES", mwsdes)).and(expressionFactory.eq("DOOBV1", inCUNO.trim()))
            }
        
            expression = expression.and(expressionFactory.eq("DOCONO", program.LDAZD.get("CONO") as String))
        
            // Build and execute the query
            DBAction query = database.table("DRODPR")
                .index("00")
                .matching(expression)
                .selectAllFields()
                .build()
            
            DBContainer container = query.getContainer()
            def myList = []
        
            // Read all records
            query.readAll(container, 0, max_records) { DBContainer container1 ->
                def myObject = [
                    DOCONO: container1.get("DOCONO")?.toString(),
                    DOEDES: container1.get("DOEDES")?.toString(),
                    DOROUT: container1.get("DOROUT")?.toString(),
                    //DRTX40: container1.get("DRTX40")?.toString()
                ]
                myList.add(myObject)
            }
            
            arrayOFRoutesAllowforCuno=myList

        // arrayOFRoutes=myList
        // retrieveItemQtyAtWarehouse()
        // retrieveDurationFromRoute()
        }
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
        void retrieveRouteFromPlaceofLoading() {
          retrieveRouteFromPlaceofLoadingAndCustomer()
            int max_records = mi.getMaxRecords() <= 0 ? 10000 : mi.getMaxRecords()
            ExpressionFactory expressionFactory = database.getExpressionFactory("DROUTE")
            //def placeOfLoadingValues = arrayofPlaceofLoading.collect { it?.MWSDES?.trim() }.findAll { it }
            
            if (arrayOFRoutesAllowforCuno.isEmpty()) {
                return
            }
            
          // Start with the first element's conditions
          def expression = expressionFactory.eq("DRSDES", arrayOFRoutesAllowforCuno[0]['DOEDES'])
          expression = expression.and(expressionFactory.eq("DRROUT", arrayOFRoutesAllowforCuno[0]['DOROUT']))
          
          // Loop through the remaining elements
          arrayOFRoutesAllowforCuno.drop(1).each { element ->
              def tempExpression = expressionFactory.eq("DRSDES", element.DOEDES)
              tempExpression = tempExpression.and(expressionFactory.eq("DRROUT", element.DOROUT))
              
              // Combine the current expression with the new one using OR
              expression = expression.or(tempExpression)
          }
          
          // Finally, add the DRCONO condition
          expression = expression.and(expressionFactory.eq("DRCONO", program.LDAZD.get("CONO") as String))

            // Build and execute the query
            DBAction query = database.table("DROUTE")
                .index("00")
                .matching(expression)
                .selectAllFields()
                .build()
            
            DBContainer container = query.getContainer()
            def myList = []
        
            // Read all records
            query.readAll(container, 0, max_records) { DBContainer container1 ->
                def myObject = [
                    DRCONO: container1.get("DRCONO")?.toString(),
                    DRSDES: container1.get("DRSDES")?.toString(),
                    DRROUT: container1.get("DRROUT")?.toString(),
                    DRTX40: container1.get("DRTX40")?.toString()
                ]
                myList.add(myObject)
            }

        arrayOFRoutes=myList
        retrieveItemQtyAtWarehouse()
        retrieveDurationFromRoute()
        }
        
    void retrieveItemQtyAtWarehouse() {
        int max_records = mi.getMaxRecords() <= 0 ? 100 : mi.getMaxRecords()
        ExpressionFactory expression = database.getExpressionFactory("MITBAL")
        def whloValues = listresponseMMS059_ListGlobal*.FWHL.collect { it?.trim() }.findAll { it }
        def uniqueWhloValues = whloValues.unique()
        
        expression = expression.eq("MBWHLO", uniqueWhloValues[0])
        for (value in uniqueWhloValues) {
            expression = expression.or(expression.eq("MBWHLO", value.toString()))
        }
    
        expression = expression.and(expression.eq("MBCONO", program.LDAZD.get("CONO") as String))
        if(inITNO!=""){
        expression = expression.and(expression.eq("MBITNO",inITNO ))
        }
        else{
        expression = expression.and(expression.eq("MBITNO",ITNOpop5or4 ))
        }
    
        DBAction query = database.table("MITBAL")
            .index("00")
            .matching(expression)
            .selectAllFields()
            .build()
        
        DBContainer container = query.getContainer()
        def myList = []
    
        // Read all records
        query.readAll(container, 0, max_records) { DBContainer container1 ->
            def myObject = [
                MBCONO: container1.get("MBCONO")?.toString(),
                MBWHLO: container1.get("MBWHLO")?.toString(),
                MBTOMU: container1.get("MBTOMU")?.toString(),
                MBALQT: container1.get("MBALQT")?.toString(),
                MBAVAL: container1.get("MBAVAL")?.toString()
            ]
            myList.add(myObject)
            arrayOFWarehouseQTY=myList
        }
    }   

    void retrieveDurationFromRoute() {
        int max_records = mi.getMaxRecords() <= 0 ? 10000 : mi.getMaxRecords()
        ExpressionFactory expressionFactory = database.getExpressionFactory("DROUDI")
    
        def arrayOFRoutesValues = arrayOFRoutes.collect { it?.DRROUT?.trim() }.findAll { it }
        
        if (arrayOFRoutesValues.isEmpty()) {
            return
        }
    
        def expression = expressionFactory.eq("DSROUT", arrayOFRoutesValues[0])
    
        arrayOFRoutesValues.drop(1).each { drrout ->
            expression = expression.or(expressionFactory.eq("DSROUT", drrout))
        }
    
        expression = expression.and(expressionFactory.eq("DSCONO", program.LDAZD.get("CONO") as String))
    
        DBAction query = database.table("DROUDI")
            .index("00")
            .matching(expression)
            .selectAllFields()
            .build()
        
        DBContainer container = query.getContainer()
        def myList = []
    
        // Read all records
        query.readAll(container, 0, max_records) { DBContainer container1 ->
            def myObject = [
                DSCONO: container1.get("DSCONO")?.toString(),
                DSROUT: container1.get("DSROUT")?.toString(),
                DSDDOW: container1.get("DSDDOW")?.toString(),
                DSARHH: container1.get("DSARHH")?.toString()?.padLeft(2, '0'),
                DSARMM: container1.get("DSARMM")?.toString()?.padLeft(2, '0'),
                DSLILH: container1.get("DSLILH")?.toString()?.padLeft(2, '0'),
                DSLILM: container1.get("DSLILM")?.toString()?.padLeft(2, '0'),
               // DSDETH: container1.get("DSDETH")?.toString()?.padLeft(2, '0'),
               //DSDETM: container1.get("DSDETM")?.toString()?.padLeft(2, '0'),
                DSARDY: container1.get("DSARDY")?.toString(),
                DSRODN: container1.get("DSRODN")?.toString()
            ]
            myList.add(myObject)
            arrayOFDuration=myList
        }
        
    joinArrays()
    }   
    
    void joinArrays(){
    // Step 0: Join array0 and array1
    def joinedArray0And1 =arrayOFWarehouseQTY.collectMany { mbItem ->
        arrayofPlaceofLoading.findAll { mwItem ->
            mwItem.MWCONO == mbItem.MBCONO &&  mwItem.MWWHLO == mbItem.MBWHLO
        }.collect { mwItem ->
            [MWCONO: mwItem.MWCONO, MWWHLO: mwItem.MWWHLO, MWSDES: mwItem.MWSDES, MWWHNM: mwItem.MWWHNM, MBAVAL:mbItem.MBAVAL, MBALQT:mbItem.MBALQT,MBTOMU:mbItem.MBTOMU ]
        }
    } 
    // Step 1: Join array1 and array2
    def joinedArray1And2 = joinedArray0And1.collectMany { mwItem ->
        arrayOFRoutes.findAll { drItem ->
            drItem.DRCONO == mwItem.MWCONO && drItem.DRSDES == mwItem.MWSDES
        }.collect { drItem ->
            [MWCONO: mwItem.MWCONO, MWWHLO: mwItem.MWWHLO, MWSDES: mwItem.MWSDES, MWWHNM: mwItem.MWWHNM,
             DRROUT: drItem.DRROUT, DRTX40: drItem.DRTX40, MBAVAL: mwItem.MBAVAL, MBALQT: mwItem.MBALQT,MBTOMU: mwItem.MBTOMU ]
        }
    }
    
    // Step 2: Join the result with array3
    def finalJoinedArray = joinedArray1And2.collectMany { item1 ->
        arrayOFDuration.findAll { dsItem ->
            dsItem.DSCONO == item1.MWCONO && dsItem.DSROUT == item1.DRROUT
        }.collect { dsItem ->
            [MWCONO: item1.MWCONO, MWWHLO: item1.MWWHLO, MWSDES: item1.MWSDES, MWWHNM: item1.MWWHNM,
             DRROUT: item1.DRROUT, DRTX40: item1.DRTX40, DSDDOW: dsItem.DSDDOW, DSARHH: dsItem.DSARHH, DSARMM: dsItem.DSARMM, MBAVAL: item1.MBAVAL, MBALQT: item1.MBALQT, MBTOMU: item1.MBTOMU, DSLILH: dsItem.DSLILH, DSLILM: dsItem.DSLILM, DSARDY: dsItem.DSARDY, DSRODN: dsItem.DSRODN]
        }
    }  

      finalBigArray=finalJoinedArray

    updatedArray = finalBigArray.each { item ->
      def var1= getNextActiveDateTime(item["DSDDOW"].toString(), dateTimeZone, timeTimeZone, item["DSLILH"] as Integer, item["DSLILM"] as Integer)
      def dateYYYYMMDD=LocalDateTime.parse(var1.toString(), DateTimeFormatter.ofPattern('yyyy-MM-dd\'T\'HH:mm')).format(DateTimeFormatter.ofPattern('yyyyMMdd'))
      def newDateString = LocalDate.parse(dateYYYYMMDD as String, DateTimeFormatter.ofPattern('yyyyMMdd')).plusDays(item["DSARDY"] as Integer).format(DateTimeFormatter.ofPattern('yyyyMMdd'))
      item["DSDT"]  = dateYYYYMMDD
      item["DSDDOW"] =newDateString //added ardy with departure date
        return item
    }

    sortArrayByDateTime()
    }

void sortArrayByDateTime() {
    def sortedData = updatedArray.collect().sort { a, b ->
        a["DSDDOW"] as Integer <=> b["DSDDOW"] as Integer ?:
        a["DSARHH"] as Integer <=> b["DSARHH"] as Integer ?:
        a["DSARMM"] as Integer <=> b["DSARMM"] as Integer
    }

    // Keep only the first occurrence of each DRROUT
    uniqueData=sortedData// to comment later
    //uniqueData = sortedData.unique { it["DRROUT"] }

    ///mi.outData.put("TEST", uniqueData.toString())
}


    
    
def getNextActiveDateTime(String sequence, String currentDate, String currentTime, Integer arhh, Integer armm) {
    // Ensure the sequence is 7 characters long (Monday to Sunday)
    if (sequence.length() != 7) {
        mi.error('The sequence must be 7 characters long.')
    }


    // Validate date and time formats
    if (currentDate.length() != 8 || currentTime.length() != 4) {
        mi.error('The date should be in YYYYMMDD format and time in HHMM format.')
    }
    



    def formatter = DateTimeFormatter.ofPattern('yyyyMMddHHmm')
    LocalDateTime today

    try {
        today = LocalDateTime.parse("${currentDate}${currentTime}", formatter)
        // today=LocalDateTime.parse("${currentDate}${currentTime}", formatter)
    } catch (DateTimeParseException e) {
        mi.error(e.toString())
    }

    def currentDay = today.dayOfWeek.value % 7 // Monday - 0, ..., Sunday - 6
    def sequenceArray = sequence.collect { it as int }

    // If today is Sunday (0), adjust to align with Monday as start of the week
    def adjustedDay = currentDay == 0 ? 6 : currentDay - 1


    // Check if the current day is active and if the current time is before the target time
    if (sequenceArray[adjustedDay] == 1) {

        // Example target time (you need to get this from your data)
        def targetHours = arhh
        def targetMinutes = armm
        def targetDateTime = createDateWithTime(today, targetHours, targetMinutes)
        //mi.outData.put("TEST", "target date:"+targetDateTime.toString()+" today:"+today.toString())
        if (!(targetDateTime as LocalDateTime).isBefore(today)) {
            return targetDateTime
        }


    }

    // Find the next active day
    for (i in 1..6) {
        def nextDay = (adjustedDay + i) % 7
        if (sequenceArray[nextDay] == 1 ) {
            def nextDate = today.plusDays(i)
            // Example target time (you need to get this from your data)
            def targetHours = arhh
            def targetMinutes = armm
            def targetDateTime = createDateWithTime(nextDate, targetHours, targetMinutes)
            // mi.error(targetDateTime.toString())
            return targetDateTime
            
        }
    }

    // If no valid date and time is found
    return null
}

    def createDateWithTime(LocalDateTime date, int hours, int minutes) {
        return date.withHour(hours).withMinute(minutes).withSecond(0)
        // return "20241101".toString()
    }

    void ifWHLOisnotEmpty(String whlo){
    // if(inOALT!=1 && inOALT!=0){
    // if (!MMS200_GetSumWhsBalApiCall(whlo, ITNOpop5or4)) return
    // }
    if(inSPLM!=""){
      MMS059_ListApiCall(inSPLM)
      PADL610_059 = PADL610
      BCKO610_059 = BCKO610
    }else if(SPLM610.trim()!=""){
      MMS059_ListApiCall(SPLM610.trim())
      PADL610_059 = PADL610
      BCKO610_059 = BCKO610      
    }
    else{
         MMS059_ListApiCall("")
    }

      if(!listresponseMMS059_ListGlobal.isEmpty()){
      PADL610_059 = listresponseMMS059_ListGlobal[0].PADL;
      BCKO610_059 = listresponseMMS059_ListGlobal[0].BCKO;
      
      }
      else if(listresponseMMS059_ListGlobal.isEmpty()){
      mi.error("Supply Model "+SPLM610.trim()+" does not exist which is configured for client "+ inCUNO.trim())
      return
      }    

          if(inROUT!=""){
          DRS005_GetRouteApiCall(inROUT)
          if(!listresponseDRS005_GetRouteGlobal.isEmpty()){
          }else{
          mi.error("Route "+inROUT+" does not exist") 
          return
          }
          
          }  

        retrievePlaceOfLoadingFromWarehouse(whlo)
        if(inROUT!=""){
        def priorityRout = uniqueData.find { it["DRROUT"].toString().trim() == inROUT }
        def otherRout = uniqueData.findAll { it["DRROUT"].toString().trim() != inROUT }
        def uniqueDataPriority = (priorityRout ? [priorityRout] : []) + otherRout
        

        
        uniqueDataPriority.each { item ->
        mi.outData.put("TEST", uniqueDataPriority.toString())





        mi.outData.put("CUNO", inCUNO.trim())
        mi.outData.put("ORTP", ORTP610.trim())
        mi.outData.put("ITNO", ITNOpop5or4.trim())
        if(inPOP4.trim()!=""){
        mi.outData.put("POP4", inPOP4.trim())
        }
        if(inPOP5.trim()!=""){
        mi.outData.put("POP5", inPOP5.trim())
        }
        //mi.outData.put("AV01", AV01_MMS200.trim())
        mi.outData.put("AV01", (item["MBAVAL"]?.toString().trim() as Double)-(item["MBALQT"]?.toString().trim() as Double) as String)
        mi.outData.put("CFI1", inCFI1.trim())
        if(inORQA!=-1){
          mi.outData.put("ORQA", inORQA.toString().trim())
        }
        mi.outData.put("WHLO", item["MWWHLO"]?.toString().trim())
        if(inSPLM.trim()!=""){
        mi.outData.put("SPLM", inSPLM.trim())
        }
        else{
         mi.outData.put("SPLM", SPLM610.trim()) 
        }

      mi.outData.put("WHNM", item["MWWHNM"]?.toString().trim())
      //mi.outData.put("TOMU", TOMU_MMS200.trim())
      mi.outData.put("TOMU", (item["MBTOMU"]?.toString()?.trim()?.toDouble() ?: 0.0).intValue().toString())
      mi.outData.put("PADL", PADL610_059.trim())
      mi.outData.put("BCKO", BCKO610_059.trim())
      mi.outData.put("TX40", item["DRTX40"]?.toString().trim())
      mi.outData.put("ROUT", item["DRROUT"]?.toString().trim())
      mi.outData.put("CODZ", item["DSDDOW"]?.toString().trim())
      mi.outData.put("COHZ", "${item["DSARHH"]?.toString().trim()}${item["DSARMM"]?.toString().trim()}")
              mi.write()
        }
        }else if(inOALT==2){
        def priorityRout = uniqueData.find { it["DRROUT"].toString().trim() == inROUT }
        def otherRout = uniqueData.findAll { it["DRROUT"].toString().trim() != inROUT }
        def uniqueDataPriority = (priorityRout ? [priorityRout] : []) + otherRout
        def filteredArray
        if(uniqueDataPriority[0]) {
            def firstElement = uniqueDataPriority[0]
            def dsdow = firstElement["DSDDOW"]?.toString()?.trim()
            def DSARHH = firstElement["DSARHH"]?.toString()?.trim()
            def DSARMM = firstElement["DSARMM"]?.toString()?.trim()
            filteredArray = uniqueDataPriority.findAll { it["DSDDOW"].toString().trim() == dsdow && it["DSARHH"].toString().trim() == DSARHH && it["DSARMM"].toString().trim() == DSARMM }
        }

        // Filter the array based on matching DSDDOW, DSARHH, and DSARMM values
        //def filteredArray = uniqueDataPriority.findAll { it["DSDDOW"].toString().trim() == dsdow && it["DSARHH"].toString().trim() == DSARHH && it["DSARMM"].toString().trim() == DSARMM }

        filteredArray.each { item ->
        //println "DRROUT: ${item["DRROUT"]}, DSDDOW: ${item["DSDDOW"]}, DSARHH: ${item["DSARHH"]}, DSARMM: ${item["DSARMM"]}"
        mi.outData.put("CUNO", inCUNO.trim())
        mi.outData.put("ORTP", ORTP610.trim())
        mi.outData.put("ITNO", ITNOpop5or4.trim())
        if(inPOP4.trim()!=""){
        mi.outData.put("POP4", inPOP4.trim())
        }
        if(inPOP5.trim()!=""){
        mi.outData.put("POP5", inPOP5.trim())
        }
        //mi.outData.put("AV01", AV01_MMS200.trim())
        mi.outData.put("AV01", (item["MBAVAL"]?.toString().trim() as Double)-(item["MBALQT"]?.toString().trim() as Double) as String)
        mi.outData.put("CFI1", inCFI1.trim())
        if(inORQA!=-1){
          mi.outData.put("ORQA", inORQA.toString().trim())
        }
        mi.outData.put("WHLO", item["MWWHLO"]?.toString().trim())
        if(inSPLM.trim()!=""){
        mi.outData.put("SPLM", inSPLM.trim())
        }
        else{
         mi.outData.put("SPLM", SPLM610.trim()) 
        }
      mi.outData.put("WHNM", item["MWWHNM"]?.toString().trim())
      //mi.outData.put("TOMU", TOMU_MMS200.trim())
      mi.outData.put("TOMU", (item["MBTOMU"]?.toString()?.trim()?.toDouble() ?: 0.0).intValue().toString())
      mi.outData.put("PADL", PADL610_059.trim())
      mi.outData.put("BCKO", BCKO610_059.trim())
      mi.outData.put("TX40", item["DRTX40"]?.toString().trim())
      mi.outData.put("ROUT", item["DRROUT"]?.toString().trim())
      mi.outData.put("RODN", item["DSRODN"]?.toString().trim())
      mi.outData.put("DSDT", item["DSDT"]?.toString().trim())
      mi.outData.put("CODZ", item["DSDDOW"]?.toString().trim())
      mi.outData.put("COHZ", "${item["DSARHH"]?.toString().trim()}${item["DSARMM"]?.toString().trim()}")
      mi.outData.put("DSHM", "${item["DSLILH"]?.toString().trim()}${item["DSLILH"]?.toString().trim()}")
      mi.outData.put("IDEX", "1")
      mi.outData.put("IDWH", "1")
      mi.write()
        }          
          
        }else if(inOALT==0){
          

       
        def sortedDataByWHLO_DDOW_ARHH_ARMM = uniqueData.collect().sort { a, b ->
            // Ensure MWWHLO is compared as a String
            def result = (a['MWWHLO'] as String) <=> (b['MWWHLO'] as String)
            
            // If MWWHLO is equal, compare DSDDOW
            if (result == 0) {
                def dateA = (a['DSDDOW'] as String).padLeft(8, '0') // Ensure it's an 8-digit date string
                def dateB = (b['DSDDOW'] as String).padLeft(8, '0') // Ensure it's an 8-digit date string
                result = dateA <=> dateB
            }
            
            // If DSDDOW is also equal, compare DSARHH
            if (result == 0) {
                def hoursA = (a['DSARHH'] as Integer).toString().padLeft(2, '0') // Ensure it's a 2-digit hour
                def hoursB = (b['DSARHH'] as Integer).toString().padLeft(2, '0') // Ensure it's a 2-digit hour
                result = hoursA <=> hoursB
            }
            
            // If DSARHH is also equal, compare DSARMM
            if (result == 0) {
                def minutesA = (a['DSARMM'] as Integer).toString().padLeft(2, '0') // Ensure it's a 2-digit minute
                def minutesB = (b['DSARMM'] as Integer).toString().padLeft(2, '0') // Ensure it's a 2-digit minute
                result = minutesA <=> minutesB
            }
            
            return result
        }
        
        
        
        def idexCounter = 1
        def lastMWWHLO = ''
        
        //Assign IDEX
        sortedDataByWHLO_DDOW_ARHH_ARMM.each { item ->
            def mwWhlo = item['MWWHLO']
        
            // Check if the MWWHLO is the same as the last one
            if (mwWhlo != lastMWWHLO) {
                idexCounter = 1
                lastMWWHLO = mwWhlo
            }
        
            // Add IDEX directly to the item map
            item['IDEX'] = idexCounter
        
            // Increment the counter for the next occurrence
            idexCounter++
        }
        //End of Assign IDEX
         
        
        //Assign IDWH
          // Step 1: Create a sorted copy of the original array based on DSDDOW, DSARHH, and DSARMM (without sorting the original)
          def sortedData = sortedDataByWHLO_DDOW_ARHH_ARMM.collect { it } // Create a shallow copy of the original array
          
          sortedData.sort { a, b ->
              (a['DSDDOW'] as Integer) <=> (b['DSDDOW'] as Integer) ?: 
              (a['DSARHH'] as Integer) <=> (b['DSARHH'] as Integer) ?: 
              (a['DSARMM'] as Integer) <=> (b['DSARMM'] as Integer)
          }
          
          // Step 2: Remove duplicates by MWWHLO and assign IDWH starting from 1
          def result = [:]
          def idwhCounter = 1
          
          sortedData.each { entry ->
              def mwhlo = entry['MWWHLO'].toString()
              if (!result.containsKey(mwhlo)) {
                  result[mwhlo] = idwhCounter++
              }
          }
          
          // Step 3: Add IDWH to each record in the sortedDataByWHLO_DDOW_ARHH_ARMM based on MWWHLO
          sortedDataByWHLO_DDOW_ARHH_ARMM.each { record ->
              String mwWHLO = record['MWWHLO'].toString()
              if (result.containsKey(mwWHLO)) {
                  record['IDWH'] = result[mwWHLO]
              }
          }
          

        //End of Assign IDWH
        


                  
          


        sortedDataByWHLO_DDOW_ARHH_ARMM.each { item ->
        //println "DRROUT: ${item["DRROUT"]}, DSDDOW: ${item["DSDDOW"]}, DSARHH: ${item["DSARHH"]}, DSARMM: ${item["DSARMM"]}"
        mi.outData.put("CUNO", inCUNO.trim())
        mi.outData.put("ORTP", ORTP610.trim())
        mi.outData.put("ITNO", ITNOpop5or4.trim())
        if(inPOP4.trim()!=""){
        mi.outData.put("POP4", inPOP4.trim())
        }
        if(inPOP5.trim()!=""){
        mi.outData.put("POP5", inPOP5.trim())
        }
        //mi.outData.put("AV01", AV01_MMS200.trim())
        mi.outData.put("AV01", (item["MBAVAL"]?.toString().trim() as Double)-(item["MBALQT"]?.toString().trim() as Double) as String)
        mi.outData.put("CFI1", inCFI1.trim())
        if(inORQA!=-1){
          mi.outData.put("ORQA", inORQA.toString().trim())
        }
        mi.outData.put("WHLO", item["MWWHLO"]?.toString().trim())
        if(inSPLM.trim()!=""){
        mi.outData.put("SPLM", inSPLM.trim())
        }
        else{
         mi.outData.put("SPLM", SPLM610.trim()) 
        }
      mi.outData.put("IDEX", item["IDEX"] as String)
      mi.outData.put("IDWH", item["IDWH"] as String)
      mi.outData.put("WHNM", item["MWWHNM"]?.toString().trim())
     // mi.outData.put("TOMU", TOMU_MMS200.trim())
      mi.outData.put("TOMU", (item["MBTOMU"]?.toString()?.trim()?.toDouble() ?: 0.0).intValue().toString())
      mi.outData.put("PADL", PADL610_059.trim())
      mi.outData.put("BCKO", BCKO610_059.trim())
      mi.outData.put("TX40", item["DRTX40"]?.toString().trim())
      mi.outData.put("ROUT", item["DRROUT"]?.toString().trim())
      mi.outData.put("DSDT", item["DSDT"]?.toString().trim())
      mi.outData.put("RODN", item["DSRODN"]?.toString().trim())
      mi.outData.put("CODZ", item["DSDDOW"]?.toString().trim())
      mi.outData.put("COHZ", "${item["DSARHH"]?.toString().trim()}${item["DSARMM"]?.toString().trim()}")
      mi.outData.put("DSHM", "${item["DSLILH"]?.toString().trim()}${item["DSLILM"]?.toString().trim()}")
      mi.outData.put("TEST", sortedDataByWHLO_DDOW_ARHH_ARMM.toString())
      mi.write()
        }
        

        
        
        }

    }
        
}
