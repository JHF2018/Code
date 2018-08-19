using Kingdee.BOS;
using Kingdee.BOS.App;
using Kingdee.BOS.App.Core.DataBase;
using Kingdee.BOS.App.Data;
using Kingdee.BOS.Contracts;
using Kingdee.BOS.Core.Metadata;
using Kingdee.BOS.Core.Permission.Objects;
using Kingdee.BOS.Orm.DataEntity;
using Kingdee.BOS.Resource;
using Kingdee.BOS.Util;
using Kingdee.K3.BD.Contracts;
using Kingdee.K3.Core.SCM.Args;
using Kingdee.K3.SCM.Contracts;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Transactions;
namespace JHF.Sal.App.Report
{
    public class SalRptCommon
    {
        public static void CreateAdvanceTable(Context ctx, string filterTable, List<SqlObject> lstTable)
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine(string.Format(" CREATE TABLE {0} ( ", filterTable));
            stringBuilder.AppendLine("  FORDERID int null");
            stringBuilder.AppendLine(" ,FBILLTYPEID nvarchar(100) null");
            stringBuilder.AppendLine(" ,FBILLTYPENAME nvarchar(100) null");
            stringBuilder.AppendLine(" ,FBILLNO nvarchar(100) null");
            stringBuilder.AppendLine(" ,FDATE Datetime null");
            stringBuilder.AppendLine(" ,FCURRENTNAME nvarchar(100) null");
            stringBuilder.AppendLine(" ,FCUSTOMERID nvarchar(100) null");
            stringBuilder.AppendLine(" ,FCUSTOMERNAME nvarchar(255) null");
            stringBuilder.AppendLine(" ,FSALEORG nvarchar(100) null");
            stringBuilder.AppendLine(" ,FSALEORGID nvarchar(100) null");
            stringBuilder.AppendLine(" ,FSALEORGNAME  nvarchar(100)   null");
            stringBuilder.AppendLine(" ,FSALEDEPTID nvarchar(100) null");
            stringBuilder.AppendLine(" ,FSALEDEPTNAME  nvarchar(255) null");
            stringBuilder.AppendLine(" ,FSALEGROUPID  nvarchar(100) null");
            stringBuilder.AppendLine(" ,FSALEGROUPNAME nvarchar(255) null");
            stringBuilder.AppendLine(" ,FSALERID  nvarchar(100) null");
            stringBuilder.AppendLine(" ,FSALERNAME nvarchar(255) null");
            stringBuilder.AppendLine(" ,FMATERIALID nvarchar(100) null");
            stringBuilder.AppendLine(" ,FMATERIALNUMBER nvarchar(100) null");
            stringBuilder.AppendLine(" ,FMATERIALNAME nvarchar(255) null");
            stringBuilder.AppendLine(" ,FMATERIALTYPE nvarchar(100) null");
            stringBuilder.AppendLine(" ,FMAPID nvarchar(100) null");
            stringBuilder.AppendLine(" ,FMAPNUMBER nvarchar(255) null");
            stringBuilder.AppendLine(" ,FMAPNAME nvarchar(255) null");
            stringBuilder.AppendLine("  ,FBillQTY  decimal(23,10)  null  ");
            stringBuilder.AppendLine("  ,FPRICE  decimal(23,10)  null  ");
            stringBuilder.AppendLine(" ,FTAXPRICE decimal(18,0) null");
            stringBuilder.AppendLine("  ,FBillAMOUNT  decimal(23,10)  null  ");
            stringBuilder.AppendLine(" ,FALLAMOUNT decimal(18,0) null");
            stringBuilder.AppendLine(" ,FDELIVERYDATE datetime null ");
            stringBuilder.AppendLine(" ,FPLANDELIVERYDATE datetime null ");
            stringBuilder.AppendLine(" ,FMRPFREEZESTATUS nvarchar(100) null ");
            stringBuilder.AppendLine(" ,FMRPTERMINATESTATUS nvarchar(100) null ");
            stringBuilder.AppendLine(" ,FCLOSESTATUS nvarchar(100) null ");
            stringBuilder.AppendLine(" ,FMRPCLOSESTATUS nvarchar(100) null ");
            stringBuilder.AppendLine(" ,FDOCUMENTSTATUS nvarchar(100) null ");
            stringBuilder.AppendLine(" ,FADVANCEAMOUNT decimal(23,10) null ");
            stringBuilder.AppendLine(" ) ");
            lstTable.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
        }
        public static void GetFilterOrder(Context ctx, string filterTable, bool bNeedAdvanceFilter, string sqlWhere, string orgList, List<BaseDataTempTable> listBaseDataTempTable, string sReportId)
        {
            string text = string.Empty;
            text = SalRptCommon.GetfilterGroupDataIsolation(ctx, orgList, new BusinessGroupDataIsolationArgs
            {
                OrgIdKey = "FSALEORGID",
                PurchaseParameterKey = "GroupDataIsolation",
                PurchaseParameterObject = "SAL_SystemParameter",
                BusinessGroupKey = "FSALEGROUPID",
                OperatorType = "XSY"
            });
            List<SqlObject> list = new List<SqlObject>();
            StringBuilder stringBuilder = new StringBuilder();
            if (bNeedAdvanceFilter)
            {
                stringBuilder.AppendLine(string.Format(" INSERT /*+append*/ INTO {0} ", filterTable));
                stringBuilder.AppendLine("(");
                stringBuilder.AppendLine(" FORDERID,FBILLTYPEID,FBILLTYPENAME,FBILLNO,FDATE,FCURRENTNAME,FCUSTOMERID,FCUSTOMERNAME,");
                stringBuilder.AppendLine(" FSALEORGID,FSALEORGNAME,FSALEDEPTID,FSALEDEPTNAME,FSALEGROUPID,FSALEGROUPNAME, ");
                stringBuilder.AppendLine(" FSALERID,FSALERNAME,FMATERIALID,FMATERIALNUMBER,FMATERIALNAME,FMATERIALTYPE,");
                stringBuilder.AppendLine(" FMAPID,FMAPNUMBER,FMAPNAME,");
                stringBuilder.AppendLine(" FBillQTY,FPRICE,FTAXPRICE,FBillAMOUNT,FALLAMOUNT,");
                stringBuilder.AppendLine(" FDELIVERYDATE,FPLANDELIVERYDATE,FMRPFREEZESTATUS,FMRPTERMINATESTATUS,");
                stringBuilder.AppendLine(" FCLOSESTATUS,FMRPCLOSESTATUS,FDOCUMENTSTATUS,FADVANCEAMOUNT");
                stringBuilder.AppendLine(")");
                stringBuilder.AppendLine("  SELECT P.FENTRYID FORDERID, V.FBILLTYPEID, BL.FNAME FBILLTYPENAME,V.FBILLNO,V.FDATE,TCL.fname FCURRENTNAME,");
                stringBuilder.AppendLine(" CUST.FNUMBER FCUSTOMERID ,CL.FNAME FCUSTOMERNAME ,ORG.FNUMBER FSALEORGID,OL.FNAME FSALEORGNAME ,");
                stringBuilder.AppendLine(" DEPT.FNUMBER FSALEDEPTID ,DL.FNAME FSALEDEPTNAME,VGR.FNUMBER FSALEGROUPID,VGL.FNAME FSALEGROUPNAME,");
                stringBuilder.AppendLine("  VSM.FNUMBER FSALERID,VSL.FNAME FSALERNAME,");
                stringBuilder.AppendLine(" P.FMATERIALID,TM.FNUMBER FMATERIALNUMBER,TML.FNAME FMATERIALNAME,TBY.FNAME FMATERIALTYPE,");
                stringBuilder.AppendLine(" P.FMAPID,VCM.FNUMBER AS FMAPNUMBER,VCML.FNAME AS FMAPNAME,");
                stringBuilder.AppendLine(" P.FQTY,PF.FPRICE,PF.FTAXPRICE,PF.FAMOUNT,PF.FALLAMOUNT,");
                stringBuilder.AppendLine(" PD.FDELIVERYDATE,PD.FPLANDELIVERYDATE,P.FMRPFREEZESTATUS,P.FMRPTERMINATESTATUS,");
                stringBuilder.AppendLine(" V.FCLOSESTATUS,P.FMRPCLOSESTATUS,V.FDOCUMENTSTATUS, RECAMOUNTTABLE.FADVANCEAMOUNT");
                stringBuilder.AppendLine(" FROM  T_SAL_ORDER V");
                stringBuilder.AppendLine(" LEFT JOIN T_SAL_ORDERENTRY P ON P.FID=V.FID");
                stringBuilder.AppendLine(" LEFT JOIN T_SAL_ORDERFIN TF ON TF.FID=P.FID");
                stringBuilder.AppendLine(" LEFT JOIN T_SAL_ORDERENTRY_F PF ON PF.FENTRYID =P.FENTRYID");
                stringBuilder.AppendLine(" LEFT JOIN T_SAL_ORDERENTRY_D PD ON PD.FENTRYID =P.FENTRYID");
                stringBuilder.AppendLine(" LEFT JOIN T_BD_MATERIAL TM ON TM.FMATERIALID=P.FMATERIALID");
                stringBuilder.AppendLine(" LEFT  JOIN V_SAL_CUSTMATMAPPING VCM  ");
                stringBuilder.AppendLine(" ON P.FMAPID=VCM.FID ");
                stringBuilder.AppendLine(" LEFT  JOIN V_SAL_CUSTMATMAPPING_L VCML  ");
                stringBuilder.AppendLine(" ON VCM.FID=VCML.FID ");
                string arg = "\r\n(SELECT TSOP.FID, SUM(TSOPE.FAMOUNT) FADVANCEAMOUNT\r\nFROM T_SAL_ORDERPLAN TSOP\r\nINNER JOIN T_SAL_ORDERPLANENTRY TSOPE ON TSOPE.FENTRYID = TSOP.FENTRYID\r\nWHERE TSOP.FNEEDRECADVANCE = 1\r\nGROUP BY TSOP.FID) RECAMOUNTTABLE";
                stringBuilder.AppendLine(string.Format(" Left JOIN {0} ON RECAMOUNTTABLE.FID = V.FID", arg));
                stringBuilder.AppendLine("   LEFT JOIN T_BD_MATERIALBASE TB ON TB.FMATERIALID=P.FMATERIALID ");
                stringBuilder.AppendLine(string.Format(" LEFT JOIN T_BD_MATERIALCATEGORY_L TBY ON TBY.FCATEGORYID=TB.FCATEGORYID AND TBY.FLOCALEID={0}", ctx.UserLocale.LCID));
                stringBuilder.AppendLine(string.Format(" LEFT JOIN T_BD_CURRENCY_L TCL ON TCL.FCURRENCYID=TF.FSETTLECURRID AND TCL.FLOCALEID={0}", ctx.UserLocale.LCID));
                stringBuilder.AppendLine(string.Format(" LEFT JOIN T_BD_MATERIAL_L TML ON TML.FMATERIALID=P.FMATERIALID AND TML.FLOCALEID={0}", ctx.UserLocale.LCID));
                stringBuilder.AppendLine(" LEFT JOIN T_BD_CUSTOMER CUST ON CUST.FCUSTID=V.FCUSTID ");
                stringBuilder.AppendLine(string.Format(" LEFT JOIN T_BD_CUSTOMER_L CL ON CL.FCUSTID=V.FCUSTID   AND CL.FLOCALEID={0}", ctx.UserLocale.LCID));
                stringBuilder.AppendLine(" LEFT JOIN T_BD_CUSTOMERGROUP CUSTG ON CUSTG.FID=CUST.FPRIMARYGROUP ");
                stringBuilder.AppendLine(string.Format(" LEFT JOIN T_BD_CUSTOMERGROUP_L CUSTGL ON CUSTGL.FID=CUSTG.FID  AND CUSTGL.FLOCALEID={0}", ctx.UserLocale.LCID));
                stringBuilder.AppendLine(" LEFT JOIN T_ORG_ORGANIZATIONS  ORG ON ORG.FORGID=V.FSALEORGID  ");
                stringBuilder.AppendLine(string.Format(" LEFT JOIN T_ORG_ORGANIZATIONS_L  OL ON OL.FORGID=V.FSALEORGID   AND OL.FLOCALEID={0}", ctx.UserLocale.LCID));
                stringBuilder.AppendLine(" LEFT JOIN T_BD_DEPARTMENT  DEPT ON DEPT.FDEPTID=V.FSALEDEPTID ");
                stringBuilder.AppendLine(string.Format(" LEFT JOIN T_BD_DEPARTMENT_L  DL ON DL.FDEPTID=V.FSALEDEPTID   AND DL.FLOCALEID={0}", ctx.UserLocale.LCID));
                stringBuilder.AppendLine(" LEFT JOIN V_BD_SALEGROUP VGR ON VGR.FENTRYID=V.FSALEGROUPID");
                stringBuilder.AppendLine(string.Format(" LEFT JOIN V_BD_SALEGROUP_L VGL ON VGL.FENTRYID=V.FSALEGROUPID  AND VGL.FLOCALEID={0}", ctx.UserLocale.LCID));
                stringBuilder.AppendLine(" LEFT JOIN V_BD_SALESMAN VSM ON VSM.FID=V.FSALERID");
                stringBuilder.AppendLine(string.Format(" LEFT JOIN V_BD_SALESMAN_L VSL ON VSL.FID=V.FSALERID  AND VSL.FLOCALEID={0}", ctx.UserLocale.LCID));
                stringBuilder.AppendLine(string.Format(" LEFT JOIN T_BAS_BILLTYPE_L BL ON BL.FBILLTYPEID=V.FBILLTYPEID AND BL.FLOCALEID={0}", ctx.UserLocale.LCID));
            }
            else
            {
                stringBuilder.AppendLine(string.Format(" INSERT /*+append*/ INTO {0} ", filterTable));
                stringBuilder.AppendLine("(FORDERID ");
                stringBuilder.AppendLine(")");
                stringBuilder.AppendLine("  SELECT P.FENTRYID FORDERID");
                stringBuilder.AppendLine(" FROM  T_SAL_ORDER V");
                stringBuilder.AppendLine(" INNER JOIN T_SAL_ORDERENTRY P ON P.FID=V.FID");
                stringBuilder.AppendLine(" INNER JOIN T_SAL_ORDERFIN TF ON TF.FID=P.FID");
                stringBuilder.AppendLine(" INNER JOIN T_SAL_ORDERENTRY_D PD ON PD.FENTRYID =P.FENTRYID");
                stringBuilder.AppendLine(" INNER JOIN T_BD_MATERIAL TM ON TM.FMATERIALID=P.FMATERIALID");
                stringBuilder.AppendLine(" INNER JOIN T_BD_MATERIALBASE TB ON TB.FMATERIALID=P.FMATERIALID ");
                stringBuilder.AppendLine(" INNER JOIN T_BD_CURRENCY TBCU ON TF.FSETTLECURRID=TBCU.FCURRENCYID");
                stringBuilder.AppendLine(string.Format(" INNER JOIN T_BD_CURRENCY_L TCL ON TCL.FCURRENCYID=TF.FSETTLECURRID AND TCL.FLOCALEID={0}", ctx.UserLocale.LCID));
                stringBuilder.AppendLine(" INNER JOIN T_BD_CUSTOMER CUST ON CUST.FCUSTID=V.FCUSTID ");
                stringBuilder.AppendLine(" INNER JOIN T_ORG_ORGANIZATIONS  ORG ON ORG.FORGID=V.FSALEORGID  ");
                stringBuilder.AppendLine(" INNER JOIN T_BD_UNIT TBUT ON TB.FBASEUNITID=TBUT.FUNITID");
                stringBuilder.AppendLine(" LEFT JOIN V_BD_SALESMAN VSM ON VSM.FID=V.FSALERID");
                stringBuilder.AppendLine(" LEFT JOIN T_BD_DEPARTMENT  DEPT ON DEPT.FDEPTID=V.FSALEDEPTID ");
                stringBuilder.AppendLine(" LEFT JOIN V_BD_SALEGROUP VGR ON VGR.FENTRYID=V.FSALEGROUPID");
            }
            if (!string.IsNullOrWhiteSpace(sqlWhere) && sqlWhere.Length != 0)
            {
                stringBuilder.AppendLine(string.Format(" WHERE 1=1 AND  {0}", sqlWhere));
            }
            if (!text.IsNullOrEmptyOrWhiteSpace())
            {
                stringBuilder.AppendLine("  AND  " + text);
            }
            IBDCommonService service = Kingdee.K3.BD.Contracts.ServiceFactory.GetService<IBDCommonService>(ctx);
            string baseDataIsolationSql = service.GetBaseDataIsolationSql(ctx, "T_BAS_RptDataRuleConfig", sReportId, listBaseDataTempTable);
            if (!baseDataIsolationSql.IsNullOrEmptyOrWhiteSpace())
            {
                stringBuilder.AppendLine(baseDataIsolationSql);
            }
            IMetaDataService service2 = ServiceHelper.GetService<IMetaDataService>();
            FormMetadata formMetadata = service2.Load(ctx, "SAL_SaleOrder", true) as FormMetadata;
            DataRuleFilterParamenter dataRuleFilterParamenter = new DataRuleFilterParamenter("SAL_SaleOrder")
            {
                PermissionItemId = "6e44119a58cb4a8e86f6c385e14a17ad",
                SubSystemId = formMetadata.BusinessInfo.GetForm().SubsysId,
                BusinessInfo = formMetadata.BusinessInfo
            };
            IPermissionService service3 = ServiceHelper.GetService<IPermissionService>();
            DataRuleFilterObject dataRuleFilterObject = service3.LoadDataRuleFilter(ctx, dataRuleFilterParamenter);
            if (!string.IsNullOrEmpty(dataRuleFilterObject.FilterString))
            {
                string text2 = dataRuleFilterObject.FilterString;
                foreach (DataRuleFilterDetail current in dataRuleFilterObject.Detail)
                {
                    if (!string.IsNullOrEmpty(current.DataRuleFilter))
                    {
                        text2 = text2.Replace(current.DataRuleFilter, "1=1");
                    }
                }
                if (!string.IsNullOrEmpty(text2))
                {
                    stringBuilder.AppendLine(" AND (" + text2 + ")");
                }
            }
            list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
            DBUtils.ExecuteBatchWithTime(ctx, list, 300);
        }
        public static void CreateFlowTable(Context ctx, string flowTable, List<SqlObject> lstTable)
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine(string.Format(" CREATE TABLE {0}", flowTable));
            stringBuilder.AppendLine("(");
            stringBuilder.AppendLine(" FSEQ INT NULL ");
            stringBuilder.AppendLine(" ,FGROUPID INT NULL ");
            stringBuilder.AppendLine(" ,FORDERID INT NULL");
            stringBuilder.AppendLine(" ,FBASEORDERQTY DECIMAL(23,10) ");
            stringBuilder.AppendLine(" ,FBASECAlCQTY DECIMAL(23,10) ");
            stringBuilder.AppendLine(" ,FDENOID INT NULL");
            stringBuilder.AppendLine(" ,FBASEDENOQTY DECIMAL(23,10) ");
            stringBuilder.AppendLine(" ,FOUTID INT NULL");
            stringBuilder.AppendLine(" ,FOUTTYPE INT NULL");
            stringBuilder.AppendLine(" ,FBASEOUTQTY DECIMAL(23,10) ");

            stringBuilder.AppendLine(" ,FMOID INT NULL");//生产订单entryID
            stringBuilder.AppendLine(" ,FBASEMOQTY DECIMAL(23,10) ");//生产订单分录数量
            stringBuilder.AppendLine(" ,FPRDINSTOCKID INT NULL");//生产入单entryID
            stringBuilder.AppendLine(" ,FBASEPRDINQTY DECIMAL(23,10) ");//生产入库分录数量
            stringBuilder.AppendLine(" ,FPURORDERID INT NULL");//采购订单分录ID
            stringBuilder.AppendLine(" ,FBASEPURORDERQTY DECIMAL(23,10) ");//采购订单分录数量
            stringBuilder.AppendLine(" ,FPURINSTOCKID INT NULL");//采购订单入库ID
            stringBuilder.AppendLine(" ,FBASEPURINSTOCKQTY DECIMAL(23,10) ");//采购订单入库数量
            stringBuilder.AppendLine(" ,FPLANID INT NULL");//计划订单入库ID
            stringBuilder.AppendLine(" ,FBASEPLANQTY DECIMAL(23,10) ");//计划订单入库数量
            stringBuilder.AppendLine(" ,FOPEID INT NULL");//工序ID
            stringBuilder.AppendLine(" ,FBASEOPEQTY DECIMAL(23,10) ");//工序数量


            stringBuilder.AppendLine(" ,FRETNOID INT NULL");
            stringBuilder.AppendLine(" ,FBASERETQTY DECIMAL(23,10) ");
            stringBuilder.AppendLine(" ,FRETURNSID INT NULL");
            stringBuilder.AppendLine(" ,FRETURNID INT NULL");
            stringBuilder.AppendLine(" ,FRETURNTYPE INT NULL");
            stringBuilder.AppendLine(" ,FBASERETURNQTY DECIMAL(23,10) ");
            stringBuilder.AppendLine(" ,FSOCID INT NULL");
            stringBuilder.AppendLine(" ,FBASESOCQTY DECIMAL(23,10) ");
            stringBuilder.AppendLine(" ,FSICID INT NULL");
            stringBuilder.AppendLine(" ,FBASESICQTY DECIMAL(23,10) ");
            stringBuilder.AppendLine(" ,FRECSID INT NULL");
            stringBuilder.AppendLine(" ,FRECID INT NULL");
            stringBuilder.AppendLine(" ,FOPENQTY DECIMAL(23,10) ");
            stringBuilder.AppendLine(" ,FBASERECQTY DECIMAL(23,10) ");
            stringBuilder.AppendLine(" ,FRECPRICEQTY DECIMAL(23,10) ");
            stringBuilder.AppendLine(" )");
            lstTable.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
        }
        public static void InsertFlowData(Context ctx, string flowTable, List<string> lstTable, string flowData, string filterSql, long[] entryIds, bool bIncludedUnfilledOrders)
        {

            //innsert语句
            StringBuilder insertSbSql = new StringBuilder();

            //select 字段语句
            StringBuilder selectSbSql = new StringBuilder();
            //过滤 where语句拼接
            StringBuilder whereSql = new StringBuilder();
            StringBuilder stringBuilder4 = new StringBuilder();
            StringBuilder tableSpSql = new StringBuilder();
            List<string> list = new List<string>();
            string text = string.Empty;
            List<SqlObject> list2 = new List<SqlObject>();
            if (lstTable == null && lstTable.Count <= 0)
            {
                return;
            }
            whereSql.AppendLine("  AND ( 1=1");
            int num = 0;
            for (int i = 0; i < lstTable.Count; i++)
            {
                if (lstTable[i].ToUpperInvariant().Equals("T_SAL_INITOUTSTOCKENTRY"))
                {
                    num = i;
                    break;
                }
            }
            for (int j = 0; j < lstTable.Count; j++)
            {
                string key;
                switch (key = lstTable[j].ToUpperInvariant())
                {
                    case "T_PRD_MOENTRY":
                        insertSbSql.Append(" FMOID,FBASEMOQTY,");
                        selectSbSql.Append(string.Format(" T{0}.FENTRYID,CASE WHEN MO.FDOCUMENTSTATUS='C' THEN T{1}.FBASEUNITQTY ELSE NULL END AS FBASEMOQTY,", j, j));
                        whereSql.Append(string.Format(" AND {0}=0 ", lstTable[j]));
                        list.Add(lstTable[j]);
                        list.Add("T_PRD_MOENTRY_SID");
                        tableSpSql.Append(string.Format(" {0},", lstTable[j]));
                        break;
                    case "T_PRD_INSTOCKENTRY":
                        insertSbSql.Append(" FPRDINSTOCKID,FBASEPRDINQTY,");
                        selectSbSql.Append(string.Format(" T{0}.FENTRYID,CASE WHEN PRIN.FDOCUMENTSTATUS='C' THEN T{1}.FBASEREALQTY ELSE NULL END AS FBASEPRDINQTY,", j, j));
                        whereSql.Append(string.Format(" AND {0}=0 ", lstTable[j]));
                        list.Add(lstTable[j]);
                        list.Add("T_PRD_INSTOCKENTRY_SID");
                        tableSpSql.Append(string.Format(" {0},", lstTable[j]));
                        break;

                    case "T_PUR_POORDERENTRY":
                        insertSbSql.Append(" FPURORDERID,FBASEPURORDERQTY,");

                        selectSbSql.Append(string.Format(" T{0}.FENTRYID,CASE WHEN PUR.FDOCUMENTSTATUS='C' THEN T{1}.FBASEUNITQTY ELSE NULL END AS FBASEPURORDERQTY,", j, j));
                        whereSql.Append(string.Format(" AND {0}=0 ", lstTable[j]));
                        list.Add(lstTable[j]);
                        list.Add("T_PUR_POORDERENTRY_SID");
                        tableSpSql.Append(string.Format(" {0},", lstTable[j]));
                        break;

                    case "T_STK_INSTOCKENTRY":
                        insertSbSql.Append(" FPURINSTOCKID,FBASEPURINSTOCKQTY,");
                        selectSbSql.Append(string.Format(" T{0}.FENTRYID,CASE WHEN STK.FDOCUMENTSTATUS='C' THEN T{1}.FBASEUNITQTY ELSE NULL END AS FBASEPURINSTOCKQTY,", j, j));
                        whereSql.Append(string.Format(" AND {0}=0 ", lstTable[j]));
                        list.Add(lstTable[j]);
                        list.Add("T_STK_INSTOCKENTRY_SID");
                        tableSpSql.Append(string.Format(" {0},", lstTable[j]));
                        break;
                
               
                    case "T_SAL_ORDERENTRY":
                        insertSbSql.Append(" FORDERID,FBASEORDERQTY,FBASECAlCQTY,");
                        selectSbSql.Append(" TOL.FENTRYID,TOL.FBASEUNITQTY FBASEORDERQTY,TOL.FBASEUNITQTY FBASECAlCQTY,");
                        list.Add(lstTable[j]);
                        tableSpSql.Append(string.Format(" {0},", lstTable[j]));
                        break;
                    case "T_SAL_OUTSTOCKENTRY":
                        insertSbSql.Append(" FOUTID,FOUTTYPE,FBASEOUTQTY,");
                        selectSbSql.Append(string.Format("CASE WHEN OUT.FENTRYID IS NOT NULL  THEN T{0}.FENTRYID \r\n                                                                WHEN INITE.FENTRYID IS NOT NULL AND  INITOUT.FBILLTYPEID = '5518f5ceee8053' THEN T{2}.FENTRYID \r\n                                                            ELSE 0 END FOUTID, \r\n                                                            CASE WHEN OUT.FENTRYID IS NOT NULL THEN 1 \r\n                                                                WHEN INITE.FENTRYID IS NOT NULL AND  INITOUT.FBILLTYPEID = '5518f5ceee8053' THEN 2 \r\n                                                            ELSE 0 END FOUTTYPE, \r\n                                    CASE WHEN OUT.FENTRYID IS NOT NULL   AND OUTV.FDOCUMENTSTATUS='C' OR OUT.FSTOCKFLAG=1 THEN T{1}.FSALBASEQTY \r\n                                         WHEN INITOUT.FBILLTYPEID = '5518f5ceee8053' AND INITOUT.FDOCUMENTSTATUS='C' THEN T{2}.FSALBASEQTY  \r\n                                         ELSE NULL END AS FBASEOUTQTY,", j, j, num));
                        whereSql.Append(string.Format(" AND {0}=0 ", lstTable[j]));
                        list.Add(lstTable[j]);
                        list.Add("T_SAL_OUTSTOCKENTRY_SID");
                        text = string.Format(" T{0}.FENTRYID ASC ", j);
                        tableSpSql.Append(string.Format(" {0},", lstTable[j]));
                        break;
                  
              
                    case "T_SAL_INITOUTSTOCKENTRY":
                        whereSql.Append(string.Format(" AND {0}=0 ", lstTable[j]));
                        list.Add(lstTable[j]);
                        list.Add("T_SAL_INITOUTSTOCKENTRY_SID");
                        text = string.Format(" T{0}.FENTRYID ASC ", j);
                        tableSpSql.Append(string.Format(" {0},", lstTable[j]));
                        break;
                }
            }
            whereSql.AppendLine(" )");
            #region 删除重复数据

            stringBuilder4.AppendLine(string.Format(" DELETE FROM {0} ", flowData));
            stringBuilder4.AppendLine(" WHERE 1 =1 ");
            stringBuilder4.AppendLine(whereSql.ToString());
            list2.Add(new SqlObject(stringBuilder4.ToString(), new List<SqlParam>()));
            stringBuilder4.Clear();

            #endregion
            #region 插入无流程的数据

            stringBuilder4.AppendLine(string.Format("INSERT /*+append*/ INTO {0} ", flowData));
            stringBuilder4.AppendLine("( T_SAL_ORDERENTRY,T_SAL_ORDERENTRY_SID) ");
            stringBuilder4.AppendLine(filterSql);
            stringBuilder4.AppendLine(string.Format("  AND NOT EXISTS (select 1 from {0} T2 WHERE T2.T_SAL_ORDERENTRY=T1.FORDERID", flowData));
            stringBuilder4.AppendLine(" )");
            list2.Add(new SqlObject(stringBuilder4.ToString(), new List<SqlParam>()));

            #endregion

            stringBuilder4.Clear();
            stringBuilder4.AppendLine(string.Format(" INSERT /*+append*/ INTO {0}", flowTable));
            stringBuilder4.AppendLine(" (");
            stringBuilder4.AppendLine(insertSbSql.ToString());
            stringBuilder4.AppendLine(" FSEQ ");
            stringBuilder4.AppendLine(" )");
            stringBuilder4.AppendLine(" SELECT ");
            stringBuilder4.AppendLine(selectSbSql.ToString());
            stringBuilder4.AppendLine(" ROW_NUMBER() OVER (ORDER BY TOL.FID ASC,TOL.FSEQ ASC  )");
            stringBuilder4.AppendLine(string.Format(" FROM (SELECT DISTINCT {0} FROM {1})  V ", string.Join(",", list.ToArray()), flowData));
            stringBuilder4.AppendLine(" LEFT JOIN T_SAL_ORDERENTRY TOL ON TOL.FENTRYID=V.T_SAL_ORDERENTRY");
            for (int k = 0; k < lstTable.Count; k++)
            {
                string text2 = lstTable[k];
                string text3 = lstTable[k] + "_LK";
                string text4 = lstTable[k] + "_SID";
               // string a;

                #region 拼接

                switch (text2.ToUpperInvariant())
                {
                    case "T_SAL_INITOUTSTOCKENTRY":
                        #region T_SAL_INITOUTSTOCKENTRY

                        stringBuilder4.AppendLine(string.Format(" LEFT JOIN {0} T{1} ON ( T{2}.FENTRYID=V.{3}  AND  T{4}.FSID=V.{5})", new object[]
                                 {
                                                text3,
                                                k,
                                                k,
                                                text2,
                                                k,
                                                text4
                                 }));
                        stringBuilder4.AppendLine(string.Format(" LEFT JOIN T_SAL_INITOUTSTOCKENTRY INITE ON INITE.FENTRYID=V.{0} ", text2));
                        stringBuilder4.AppendLine(" LEFT JOIN T_SAL_INITOUTSTOCK  INITOUT ON INITE.FID=INITOUT.FID ");

                        #endregion
                        break;
                    case "T_SAL_OUTSTOCKENTRY":
                        #region  T_SAL_OUTSTOCKENTRY
                        stringBuilder4.AppendLine(string.Format(" LEFT JOIN {0} T{1} ON ( T{2}.FENTRYID=V.{3}  AND  T{4}.FSID=V.{5})", new object[]
                        {
                                        text3,
                                        k,
                                        k,
                                        text2,
                                        k,
                                        text4
                        }));
                        stringBuilder4.AppendLine(string.Format(" LEFT JOIN T_SAL_OUTSTOCKENTRY OUT ON OUT.FENTRYID=V.{0} ", text2));
                        stringBuilder4.AppendLine(" LEFT JOIN T_SAL_OUTSTOCK  OUTV ON OUTV.FID=OUT.FID ");
                        #endregion
                        break;

                    case "T_PRD_MOENTRY":
                        #region T_PRD_MOENTRY


                        stringBuilder4.AppendLine(string.Format(" LEFT JOIN {0} T{1} ON ( T{2}.FENTRYID=V.{3}  AND  T{4}.FSID=V.{5})", new object[]
                        {
                                    text3,
                                    k,
                                    k,
                                    text2,
                                    k,
                                    text4
                        }));
                        stringBuilder4.AppendLine(string.Format(" LEFT JOIN T_PRD_MOENTRY MOE ON MOE.FENTRYID=V.{0} ", text2));
                        stringBuilder4.AppendLine(" LEFT JOIN T_PRD_MO  MO ON MOE.FID=MO.FID ");
                        #endregion
                        break;

                    case "T_PRD_INSTOCKENTRY":
                        #region T_PRD_INSTOCKENTRY


                        stringBuilder4.AppendLine(string.Format(" LEFT JOIN {0} T{1} ON ( T{2}.FENTRYID=V.{3}  AND  T{4}.FSID=V.{5})", new object[]
                        {
                                    text3,
                                    k,
                                    k,
                                    text2,
                                    k,
                                    text4
                        }));
                        stringBuilder4.AppendLine(string.Format(" LEFT JOIN T_PRD_INSTOCKENTRY PRI ON PRI.FENTRYID=V.{0} ", text2));
                        stringBuilder4.AppendLine(" LEFT JOIN T_PRD_INSTOCK PRIN ON PRIN.FID=PRI.FID ");
                        #endregion
                        break;
                

                    case "T_PUR_POORDERENTRY":
                        #region T_PUR_POORDERENTRY


                        stringBuilder4.AppendLine(string.Format(" LEFT JOIN {0} T{1} ON ( T{2}.FENTRYID=V.{3}  AND  T{4}.FSID=V.{5})", new object[]
                        {
                                    text3,
                                    k,
                                    k,
                                    text2,
                                    k,
                                    text4
                        }));
                        stringBuilder4.AppendLine(string.Format(" LEFT JOIN T_PUR_POORDERENTRY PURE ON PURE.FENTRYID=V.{0} ", text2));
                        stringBuilder4.AppendLine(" LEFT JOIN T_PUR_POORDER  PUR ON PUR.FID=PURE.FID ");
                        #endregion
                        break;


                    case "T_STK_INSTOCKENTRY":
                        #region T_STK_INSTOCKENTRY


                        stringBuilder4.AppendLine(string.Format(" LEFT JOIN {0} T{1} ON ( T{2}.FENTRYID=V.{3}  AND  T{4}.FSID=V.{5})", new object[]
                        {
                                    text3,
                                    k,
                                    k,
                                    text2,
                                    k,
                                    text4
                        }));
                        stringBuilder4.AppendLine(string.Format(" LEFT JOIN T_STK_INSTOCKENTRY STKE ON STKE.FENTRYID=V.{0} ", text2));
                        stringBuilder4.AppendLine(" LEFT JOIN T_STK_INSTOCK  STK ON STK.FID=STKE.FID ");
                        #endregion
                        break;




                }

                #endregion


                #region 拼接表sql


                //if (!(a == "T_SAL_DELIVERYNOTICEENTRY"))
                //{
                //    if (!(a == "T_SAL_RETURNNOTICEENTRY"))
                //    {
                //        if (!(a == "T_SAL_RETURNSTOCKENTRY"))
                //        {
                //            if (!(a == "T_SAL_OUTSTOCKENTRY"))
                //            {
                //                if (!(a == "T_AR_RECEIVABLEENTRY"))
                //                {
                //                    #region T_SAL_INITOUTSTOCKENTRY
                //                    if (a == "T_SAL_INITOUTSTOCKENTRY")
                //                    {
                //                        stringBuilder4.AppendLine(string.Format(" LEFT JOIN {0} T{1} ON ( T{2}.FENTRYID=V.{3}  AND  T{4}.FSID=V.{5})", new object[]
                //                        {
                //                                    text3,
                //                                    k,
                //                                    k,
                //                                    text2,
                //                                    k,
                //                                    text4
                //                        }));
                //                        stringBuilder4.AppendLine(string.Format(" LEFT JOIN T_SAL_INITOUTSTOCKENTRY INITE ON INITE.FENTRYID=V.{0} ", text2));
                //                        stringBuilder4.AppendLine(" LEFT JOIN T_SAL_INITOUTSTOCK  INITOUT ON INITE.FID=INITOUT.FID ");
                //                    }
                //                    #endregion
                //                }
                //                else
                //                {
                //                    #region T_AR_RECEIVABLEENTRY

                //                    stringBuilder4.AppendLine(string.Format(" LEFT JOIN {0} T{1} ON ( T{2}.FENTRYID=V.{3}  AND  T{4}.FSID=V.{5})", new object[]
                //                    {
                //                                text3,
                //                                k,
                //                                k,
                //                                text2,
                //                                k,
                //                                text4
                //                    }));
                //                    stringBuilder4.AppendLine(string.Format(" LEFT JOIN T_AR_RECEIVABLEENTRY TAR ON TAR.FENTRYID=V.{0}", text2));
                //                    stringBuilder4.AppendLine(" LEFT JOIN T_AR_RECEIVABLEENTRY_O  TARO ON TARO.FENTRYID=TAR.FENTRYID ");
                //                    stringBuilder4.AppendLine(" LEFT JOIN T_AR_RECEIVABLE  TARH ON TARH.FID=TAR.FID ");

                //                    #endregion
                //                }
                //            }
                //            else
                //            {
                //                #region  T_SAL_OUTSTOCKENTRY
                //                stringBuilder4.AppendLine(string.Format(" LEFT JOIN {0} T{1} ON ( T{2}.FENTRYID=V.{3}  AND  T{4}.FSID=V.{5})", new object[]
                //                {
                //                            text3,
                //                            k,
                //                            k,
                //                            text2,
                //                            k,
                //                            text4
                //                }));
                //                stringBuilder4.AppendLine(string.Format(" LEFT JOIN T_SAL_OUTSTOCKENTRY OUT ON OUT.FENTRYID=V.{0} ", text2));
                //                stringBuilder4.AppendLine(" LEFT JOIN T_SAL_OUTSTOCK  OUTV ON OUTV.FID=OUT.FID ");
                //                #endregion
                //            }
                //        }
                //        else
                //        {
                //            #region T_SAL_RETURNSTOCKENTRY


                //            stringBuilder4.AppendLine(string.Format(" LEFT JOIN {0} T{1} ON ( T{2}.FENTRYID=V.{3}  AND  T{4}.FSID=V.{5})", new object[]
                //            {
                //                        text3,
                //                        k,
                //                        k,
                //                        text2,
                //                        k,
                //                        text4
                //            }));
                //            stringBuilder4.AppendLine(string.Format(" LEFT JOIN T_SAL_RETURNSTOCKENTRY RET ON RET.FENTRYID=V.{0} ", text2));
                //            stringBuilder4.AppendLine(" LEFT JOIN T_SAL_RETURNSTOCK  REV ON REV.FID=RET.FID ");
                //            #endregion
                //        }
                //    }
                //    else
                //    {
                //        #region T_SAL_RETURNNOTICEENTRY


                //        stringBuilder4.AppendLine(string.Format(" LEFT JOIN {0} T{1} ON ( T{2}.FENTRYID=V.{3}  AND  T{4}.FSID=V.{5})", new object[]
                //        {
                //                    text3,
                //                    k,
                //                    k,
                //                    text2,
                //                    k,
                //                    text4
                //        }));
                //        stringBuilder4.AppendLine(string.Format(" LEFT JOIN T_SAL_RETURNNOTICEENTRY RENO ON RENO.FENTRYID=V.{0} ", text2));
                //        stringBuilder4.AppendLine(" LEFT JOIN T_SAL_RETURNNOTICE  RV ON RV.FID=RENO.FID ");
                //        #endregion
                //    }
                //}
                //else
                //{
                //    #region T_SAL_DELIVERYNOTICEENTRY

                //    stringBuilder4.AppendLine(string.Format(" LEFT JOIN {0} T{1} ON ( T{2}.FENTRYID=V.{3}  AND  T{4}.FSID=V.{5})", new object[]
                //    {
                //                text3,
                //                k,
                //                k,
                //                text2,
                //                k,
                //                text4
                //    }));
                //    stringBuilder4.AppendLine(string.Format(" LEFT JOIN T_SAL_DELIVERYNOTICEENTRY DU ON DU.FENTRYID=V.{0} ", text2));
                //    stringBuilder4.AppendLine(" LEFT JOIN T_SAL_DELIVERYNOTICE  DV ON DV.FID=DU.FID ");

                //    #endregion
                //}
                #endregion
            }



            if (string.IsNullOrWhiteSpace(text))
            {
                stringBuilder4.AppendLine(" ORDER BY TOL.FID ASC,TOL.FSEQ ASC ");
            }
            else
            {
                stringBuilder4.AppendLine(string.Format(" ORDER BY TOL.FID ASC,TOL.FSEQ ASC ,{0} ", text));
            }
            list2.Add(new SqlObject(stringBuilder4.ToString(), new List<SqlParam>()));
            stringBuilder4.Clear();
            insertSbSql.Clear();
            whereSql.Clear();
            selectSbSql.Clear();
            DBUtils.ExecuteBatchWithTime(ctx, list2, 300);
            list2.Clear();
            stringBuilder4.AppendFormat(" IF NOT EXISTS (SELECT 1 FROM KSQL_INDEXES WHERE KSQL_INDNAME = 'idx_{0}_1') CREATE INDEX idx_{0}_1 ON {1} (FRECID)", flowTable.Substring(3, 22), flowTable);
            list2.Add(new SqlObject(stringBuilder4.ToString(), new List<SqlParam>()));
            stringBuilder4.Clear();
            stringBuilder4.AppendFormat(" IF NOT EXISTS (SELECT 1 FROM KSQL_INDEXES WHERE KSQL_INDNAME = 'idx_{0}_2') CREATE INDEX idx_{0}_2 ON {1} (FORDERID)", flowTable.Substring(3, 22), flowTable);
            list2.Add(new SqlObject(stringBuilder4.ToString(), new List<SqlParam>()));
            stringBuilder4.Clear();
            stringBuilder4.AppendFormat(" IF NOT EXISTS (SELECT 1 FROM KSQL_INDEXES WHERE KSQL_INDNAME = 'idx_{0}_3') CREATE INDEX idx_{0}_3 ON {1} (FDENOID)", flowTable.Substring(3, 22), flowTable);
            list2.Add(new SqlObject(stringBuilder4.ToString(), new List<SqlParam>()));
            stringBuilder4.Clear();
            stringBuilder4.AppendFormat(" IF NOT EXISTS (SELECT 1 FROM KSQL_INDEXES WHERE KSQL_INDNAME = 'idx_{0}_4') CREATE INDEX idx_{0}_4 ON {1} (FOUTID)", flowTable.Substring(3, 22), flowTable);
            list2.Add(new SqlObject(stringBuilder4.ToString(), new List<SqlParam>()));
            stringBuilder4.Clear();
            stringBuilder4.AppendFormat(" IF NOT EXISTS (SELECT 1 FROM KSQL_INDEXES WHERE KSQL_INDNAME = 'idx_{0}_5') CREATE INDEX idx_{0}_5 ON {1} (FRETNOID)", flowTable.Substring(3, 22), flowTable);
            list2.Add(new SqlObject(stringBuilder4.ToString(), new List<SqlParam>()));
            stringBuilder4.Clear();
            stringBuilder4.AppendFormat(" IF NOT EXISTS (SELECT 1 FROM KSQL_INDEXES WHERE KSQL_INDNAME = 'idx_{0}_6') CREATE INDEX idx_{0}_6 ON {1} (FRETURNID)", flowTable.Substring(3, 22), flowTable);
            list2.Add(new SqlObject(stringBuilder4.ToString(), new List<SqlParam>()));
            stringBuilder4.Clear();

            stringBuilder4.AppendFormat(" IF NOT EXISTS (SELECT 1 FROM KSQL_INDEXES WHERE KSQL_INDNAME = 'idx_{0}_7') CREATE INDEX idx_{0}_7 ON {1} (FMOID)", flowTable.Substring(3, 22), flowTable);
            list2.Add(new SqlObject(stringBuilder4.ToString(), new List<SqlParam>()));
            stringBuilder4.Clear();
            stringBuilder4.AppendFormat(" IF NOT EXISTS (SELECT 1 FROM KSQL_INDEXES WHERE KSQL_INDNAME = 'idx_{0}_8') CREATE INDEX idx_{0}_8 ON {1} (FPURORDERID)", flowTable.Substring(3, 22), flowTable);
            list2.Add(new SqlObject(stringBuilder4.ToString(), new List<SqlParam>()));
            stringBuilder4.Clear();
            stringBuilder4.AppendFormat(" IF NOT EXISTS (SELECT 1 FROM KSQL_INDEXES WHERE KSQL_INDNAME = 'idx_{0}_9') CREATE INDEX idx_{0}_9 ON {1} (FPURINSTOCKID)", flowTable.Substring(3, 22), flowTable);
            list2.Add(new SqlObject(stringBuilder4.ToString(), new List<SqlParam>()));
            stringBuilder4.Clear();
            DBUtils.ExecuteBatch(ctx, list2);
            list2.Clear();
            if (!bIncludedUnfilledOrders)
            {
                stringBuilder4.AppendLine(string.Format(" DELETE  {0}  ", flowTable));
                stringBuilder4.AppendLine(" where ");
                stringBuilder4.AppendLine(string.Format(" {0}.FBASEDENOQTY IS NULL AND {0}.FBASERETQTY IS NULL AND {0}.FBASEOUTQTY IS NULL  ", flowTable));
                stringBuilder4.AppendLine(string.Format(" AND {0}.FBASERETURNQTY IS NULL    AND {0}.FBASERECQTY IS NULL ", flowTable));
                list2.Add(new SqlObject(stringBuilder4.ToString(), new List<SqlParam>()));
                stringBuilder4.Clear();
            }
            if (ctx.DatabaseType == DatabaseType.MS_SQL_Server)
            {
                stringBuilder4.AppendLine(string.Format("/*dialect*/ merge into {0}  P using\r\n                            (SELECT MIN(FSEQ) AS FSEQ, FORDERID,\r\n               FOUTID,SUM(ISNULL(FBASEOUTQTY,0)) AS FBASEOUTQTY,FRETURNID,SUM(ISNULL(FBASERETURNQTY,0)) AS FBASERETURNQTY,\r\n               FRECID,SUM(ISNULL(FBASERECQTY,0)) AS FBASERECQTY FROM {0} GROUP BY FORDERID,FOUTID,FRETURNID,FRECID) UT\r\n                                        on (UT.FSEQ=P.FSEQ)\r\n                            WHEN MATCHED THEN UPDATE SET P.FBASEOUTQTY=UT.FBASEOUTQTY,P.FBASERETURNQTY=UT.FBASERETURNQTY,P.FBASERECQTY=UT.FBASERECQTY;", flowTable));
            }
            else
            {
                if (ctx.DatabaseType == DatabaseType.Oracle)
                {
                    stringBuilder4.AppendLine(string.Format("/*dialect*/ merge into {0}  P using\r\n                            (SELECT MIN(FSEQ) AS FSEQ, FORDERID,\r\n               FOUTID,SUM(NVL(FBASEOUTQTY,0)) AS FBASEOUTQTY,FRETURNID,SUM(NVL(FBASERETURNQTY,0)) AS FBASERETURNQTY,\r\n               FRECID,SUM(NVL(FBASERECQTY,0)) AS FBASERECQTY FROM {0} GROUP BY FORDERID,FOUTID,FRETURNID,FRECID) UT\r\n                                        on (UT.FSEQ=P.FSEQ)\r\n                            WHEN MATCHED THEN UPDATE SET P.FBASEOUTQTY=UT.FBASEOUTQTY,P.FBASERETURNQTY=UT.FBASERETURNQTY,P.FBASERECQTY=UT.FBASERECQTY", flowTable));
                }
            }
            list2.Add(new SqlObject(stringBuilder4.ToString(), new List<SqlParam>()));
            stringBuilder4.Clear();
            stringBuilder4.AppendLine(string.Format("DELETE {0} WHERE NOT EXISTS (SELECT 1 FROM   \r\n             (SELECT MIN(FSEQ) AS FSEQ FROM {0} GROUP BY FORDERID,FOUTID,FMOID,FPRDINSTOCKID,FPURORDERID,FPURINSTOCKID ) gp WHERE gp.FSEQ={0}.FSEQ)", flowTable));
            list2.Add(new SqlObject(stringBuilder4.ToString(), new List<SqlParam>()));
            stringBuilder4.Clear();
            stringBuilder4.AppendLine(string.Format(" UPDATE {0} AS P ", flowTable));
            stringBuilder4.AppendLine(" SET FBASEORDERQTY=null");
            stringBuilder4.AppendLine(string.Format(" WHERE NOT EXISTS (SELECT 1 FROM (SELECT FORDERID, MIN(FSEQ) FSEQ FROM {0}  GROUP BY FORDERID) B WHERE B.FSEQ=P.FSEQ ) ", flowTable));
            list2.Add(new SqlObject(stringBuilder4.ToString(), new List<SqlParam>()));
            stringBuilder4.Clear();
            stringBuilder4.AppendLine(string.Format(" UPDATE {0} AS P ", flowTable));
            stringBuilder4.AppendLine(" SET FDENOID=null,FBASEDENOQTY=null");
            stringBuilder4.AppendLine(string.Format(" WHERE NOT EXISTS (SELECT 1 FROM (SELECT FDENOID, MIN(FSEQ) FSEQ FROM {0}  GROUP BY FDENOID) B WHERE B.FSEQ=P.FSEQ ) ", flowTable));
            list2.Add(new SqlObject(stringBuilder4.ToString(), new List<SqlParam>()));
            stringBuilder4.Clear();
            stringBuilder4.AppendLine(string.Format(" UPDATE {0} AS P ", flowTable));
            stringBuilder4.AppendLine(" SET FOUTID=null,FOUTTYPE=null,FBASEOUTQTY=null");
            stringBuilder4.AppendLine(string.Format(" WHERE NOT EXISTS (SELECT 1 FROM (SELECT FOUTID, MIN(FSEQ) FSEQ FROM {0}  GROUP BY FOUTID,FOUTTYPE) B WHERE B.FSEQ=P.FSEQ ) ", flowTable));
            list2.Add(new SqlObject(stringBuilder4.ToString(), new List<SqlParam>()));
            stringBuilder4.Clear();
            stringBuilder4.AppendLine(string.Format(" UPDATE {0} AS P ", flowTable));
            stringBuilder4.AppendLine(" SET FRETNOID=null,FBASERETQTY=null");
            stringBuilder4.AppendLine(string.Format(" WHERE NOT EXISTS (SELECT 1 FROM (SELECT FRETNOID, MIN(FSEQ) FSEQ FROM {0}  GROUP BY FRETNOID) B WHERE B.FSEQ=P.FSEQ ) ", flowTable));
            list2.Add(new SqlObject(stringBuilder4.ToString(), new List<SqlParam>()));
            stringBuilder4.Clear();
            stringBuilder4.AppendLine(string.Format(" UPDATE {0} AS P ", flowTable));
            stringBuilder4.AppendLine(" SET FRETURNID=null,FRETURNTYPE=null,FBASERETURNQTY=null");
            stringBuilder4.AppendLine(string.Format(" WHERE NOT EXISTS (SELECT 1 FROM (SELECT FRETURNID, MIN(FSEQ) FSEQ FROM {0}  GROUP BY FRETURNID,FRETURNTYPE) B WHERE B.FSEQ=P.FSEQ ) ", flowTable));
            list2.Add(new SqlObject(stringBuilder4.ToString(), new List<SqlParam>()));
            stringBuilder4.Clear();
            stringBuilder4.AppendLine(string.Format(" UPDATE {0} AS P ", flowTable));
            stringBuilder4.AppendLine(" SET FRECID=null,FOPENQTY=null,FBASERECQTY=null");
            stringBuilder4.AppendLine(string.Format(" WHERE NOT EXISTS (SELECT 1 FROM (SELECT FRECID, MIN(FSEQ) FSEQ FROM {0}  GROUP BY FRECID) B WHERE B.FSEQ=P.FSEQ ) ", flowTable));
            list2.Add(new SqlObject(stringBuilder4.ToString(), new List<SqlParam>()));
            stringBuilder4.Clear();
            stringBuilder4.AppendLine(string.Format(" DELETE {0} ", flowTable));
            stringBuilder4.AppendLine(" WHERE FBASEORDERQTY IS NULL AND  FDENOID IS NULL AND FOUTID IS NULL AND FRETNOID IS NULL AND FRETURNID IS NULL AND FRECID IS NULL ");
            list2.Add(new SqlObject(stringBuilder4.ToString(), new List<SqlParam>()));
            stringBuilder4.Clear();
            DBUtils.ExecuteBatchWithTime(ctx, list2, 300);
            using (KDTransactionScope kDTransactionScope = new KDTransactionScope(TransactionScopeOption.Suppress))
            {
                DbOptimizationUtil dbOptimizationUtil = new DbOptimizationUtil(ctx, new HashSet<string>
                {
                    flowTable
                });
                DBUtils.ExecuteBatch(ctx, dbOptimizationUtil.GetStatisticSql(), 1);
                kDTransactionScope.Complete();
            }
        
    }
    public static long[] GetEntryIds(Context ctx, string filterTable, string moreFilter, out string filterSql)
    {
        StringBuilder stringBuilder = new StringBuilder();
        List<long> list = new List<long>();
        stringBuilder.AppendLine(string.Format("    SELECT DISTINCT FORDERID,0 AS T_SAL_ORDERENTRY_SID FROM {0} T1 WHERE 1=1 ", filterTable));
        if (moreFilter.Length > 0)
        {
            stringBuilder.AppendLine(" AND " + moreFilter);
        }
        filterSql = stringBuilder.ToString();
        using (IDataReader dataReader = DBUtils.ExecuteReader(ctx, stringBuilder.ToString()))
        {
            while (dataReader.Read())
            {
                long item = Convert.ToInt64(dataReader["FORDERID"]);
                list.Add(item);
            }
            dataReader.Close();
        }
        return list.ToArray();
    }
    public static void CreateFlowDataTable(Context ctx, string flowTable, List<SqlObject> lstTable)
    {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.AppendLine(string.Format(" CREATE TABLE {0}", flowTable));
        stringBuilder.AppendLine("(");
        stringBuilder.AppendLine("  T_SAL_ORDERENTRY INT NULL ");
        stringBuilder.AppendLine(" ,T_SAL_DELIVERYNOTICEENTRY INT NULL ");
        stringBuilder.AppendLine(" ,T_SAL_OUTSTOCKENTRY INT NULL");
        stringBuilder.AppendLine(" ,T_SAL_RETURNNOTICEENTRY INT NULL");
        stringBuilder.AppendLine(" ,T_SAL_RETURNSTOCKENTRY INT NULL");
        stringBuilder.AppendLine(" ,T_AR_RECEIVABLEENTRY INT NULL");
        stringBuilder.AppendLine(" ,T_PRD_MOENTRY INT NULL");
        stringBuilder.AppendLine(" ,T_PRD_INSTOCKENTRY INT NULL");
        stringBuilder.AppendLine(" ,T_PUR_POORDERENTRY INT NULL");

        stringBuilder.AppendLine(" )");
        lstTable.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
        stringBuilder.Clear();
    }
    public static void InsertCustomFlowData(Context ctx, string flowTable, string filterTable, string moreFilter)
    {
        List<SqlObject> list = new List<SqlObject>();
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.AppendLine(string.Format(" INSERT /*+append*/ INTO {0}", flowTable));
        stringBuilder.AppendLine(" (");
        stringBuilder.AppendLine(" FORDERID,FBASEORDERQTY,FBASECAlCQTY");
        stringBuilder.AppendLine(" ,FSEQ ");
        stringBuilder.AppendLine(" )");
        stringBuilder.AppendLine(" SELECT FORDERID,T3.FBASEUNITQTY FBASEORDERQTY,T3.FBASEUNITQTY FBASECAlCQTY");
        stringBuilder.AppendLine(" ,ROW_NUMBER() OVER (ORDER BY T3.FID ASC ,T3.FSEQ ASC  )");
        stringBuilder.AppendLine(string.Format(" FROM  ( SELECT DISTINCT FORDERID FROM {0} T1 WHERE 1=1  ", filterTable));
        if (moreFilter.Length > 0)
        {
            stringBuilder.AppendLine(" AND " + moreFilter);
        }
        stringBuilder.AppendLine(" ) T2 ");
        stringBuilder.AppendLine(" LEFT JOIN T_SAL_ORDERENTRY T3 ON T3.FENTRYID=T2.FORDERID");
        stringBuilder.AppendLine(" WHERE 1=1 ");
        stringBuilder.AppendLine(" ORDER BY T3.FID ASC ,T3.FSEQ ASC ");
        list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
        stringBuilder.Clear();
        DBUtils.ExecuteBatchWithTime(ctx, list, 300);
    }
    public static void CreateReceivableTable(string receivableTable, List<SqlObject> lstTable)
    {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.AppendLine(string.Format(" CREATE TABLE {0}", receivableTable));
        stringBuilder.AppendLine("(");
        stringBuilder.AppendLine("  FID INT NULL ");
        stringBuilder.AppendLine("  ,FRECBILLNO  nvarchar(1000) NULL ");
        stringBuilder.AppendLine(" ,FRECID INT NULL ");
        stringBuilder.AppendLine(" ,FALLAMOUNTFOR DECIMAL(23,10) NULL");
        stringBuilder.AppendLine(" ,FRECEIVEAMOUNT DECIMAL(23,10)  NULL");
        stringBuilder.AppendLine(" ,FNRECEIPTAMOUNT DECIMAL(23,10)  NULL");
        stringBuilder.AppendLine(" ,FROWAMOUNTFOR DECIMAL(23,10)  NULL");
        stringBuilder.AppendLine(" ,FCHARGEOFFBILLNO  nvarchar(1000) NULL ");
        stringBuilder.AppendLine(" ,FCHARGEOFFAMOUNT DECIMAL(23,10)  NULL");
        stringBuilder.AppendLine(" ,FSEQ INT NULL");
        stringBuilder.AppendLine(" )");
        lstTable.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
        stringBuilder.Clear();
    }
    public static void CreateReceivableHelpTable(string receivableHelpTable, List<SqlObject> lstTable)
    {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.AppendLine(string.Format(" CREATE TABLE {0}", receivableHelpTable));
        stringBuilder.AppendLine("(");
        stringBuilder.AppendLine("  FID INT NULL ");
        stringBuilder.AppendLine("  ,FRECBILLNO  nvarchar(1000) NULL ");
        stringBuilder.AppendLine(" ,FALLAMOUNTFOR DECIMAL(23,10) NULL");
        stringBuilder.AppendLine(" ,FRECEIVEAMOUNT DECIMAL(23,10)  NULL");
        stringBuilder.AppendLine(" ,FNRECEIPTAMOUNT DECIMAL(23,10)  NULL");
        stringBuilder.AppendLine(" ,FCHARGEOFFBILLNO  nvarchar(1000) NULL ");
        stringBuilder.AppendLine(" ,FCHARGEOFFAMOUNT DECIMAL(23,10)  NULL");
        stringBuilder.AppendLine(" )");
        lstTable.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
        stringBuilder.Clear();
    }
    public static void CreateInvoceTable(string invoceTable, List<SqlObject> lstTable)
    {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.AppendLine(string.Format(" CREATE TABLE {0}", invoceTable));
        stringBuilder.AppendLine("(");
        stringBuilder.AppendLine("  FID INT NULL ");
        stringBuilder.AppendLine("  ,FINVOCEBILLNO  nvarchar(100) NULL ");
        stringBuilder.AppendLine(" ,FRECID INT NULL ");
        stringBuilder.AppendLine(" ,FSEQ INT NULL");
        stringBuilder.AppendLine(" )");
        lstTable.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
        stringBuilder.Clear();
    }
    public static void InsertInvoceData(Context ctx, string invoceTable, string flowTable)
    {
        List<SqlObject> list = new List<SqlObject>();
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.AppendLine(string.Format(" INSERT /*+append*/ INTO {0}", invoceTable));
        stringBuilder.AppendLine(" (");
        stringBuilder.AppendLine(" FID,FINVOCEBILLNO,FRECID");
        stringBuilder.AppendLine(" ,FSEQ ");
        stringBuilder.AppendLine(" )");
        stringBuilder.AppendLine(" SELECT TPV.fid,TARB.FTARGETBILLNO FINVOECEBILLNO ");
        stringBuilder.AppendLine(" ,TPV.FENTRYID ");
        stringBuilder.AppendLine(" ,ROW_NUMBER() OVER (ORDER BY TPV.FENTRYID  ASC )");
        stringBuilder.AppendLine(" FROM T_AR_RECEIVABLEENTRY TPV ");
        stringBuilder.AppendLine(" INNER JOIN T_AR_RECEIVABLE TV  ON TV.FID=TPV.FID   ");
        stringBuilder.AppendLine(" LEFT JOIN T_AR_BILLINGMATCHLOGENTRY TARB");
        stringBuilder.AppendLine(" ON TARB.FSRCBILLID=TPV.FID ");
        stringBuilder.AppendLine(" WHERE  FSOURCEFROMID = 'AR_receivable' AND TV.FDOCUMENTSTATUS='C'");
        stringBuilder.AppendLine(string.Format(" AND EXISTS (SELECT 1 FROM {0} TL WHERE TL.FRECID=TPV.FENTRYID)", flowTable));
        list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
        stringBuilder.Clear();
        DBUtils.ExecuteBatchWithTime(ctx, list, 300);
        list.Clear();
        stringBuilder.AppendFormat(" IF NOT EXISTS (SELECT 1 FROM KSQL_INDEXES WHERE KSQL_INDNAME = 'idx_{0}_1') CREATE INDEX idx_{0}_1 ON {1} (FRECID)", invoceTable.Substring(3, 22), invoceTable);
        list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
        stringBuilder.Clear();
        stringBuilder.AppendFormat(" IF NOT EXISTS (SELECT 1 FROM KSQL_INDEXES WHERE KSQL_INDNAME = 'idx_{0}_2') CREATE INDEX idx_{0}_2 ON {1} (FSEQ)", invoceTable.Substring(3, 22), invoceTable);
        list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
        stringBuilder.Clear();
        DBUtils.ExecuteBatch(ctx, list);
        list.Clear();
    }
    public static void InsertReceivableData(Context ctx, string receivableHelpTable, string receivableTable, string flowTable)
    {
        List<SqlObject> list = new List<SqlObject>();
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.AppendLine(string.Format(" INSERT /*+append*/ INTO {0}", receivableHelpTable));
        stringBuilder.AppendLine(" (");
        stringBuilder.AppendLine(" FID,FRECBILLNO,FALLAMOUNTFOR,FRECEIVEAMOUNT,FNRECEIPTAMOUNT,FCHARGEOFFAMOUNT,FCHARGEOFFBILLNO");
        stringBuilder.AppendLine(" )");
        stringBuilder.Append(string.Format("select ARFId,RecBillNo,FALLAMOUNTFOR,\r\n\t\t\t\t         SUM(FCURWRITTENOFFAMOUNTFOR) as FCURWRITTENOFFAMOUNTFOR,SUM(FNRECEIPTAMOUNT) as FNRECEIPTAMOUNT,SUM(FCHARGEOFFAMOUNT) AS FCHARGEOFFAMOUNT,FCHARGEOFFBILLNO from (\r\n\t\t\t\t         --正常情况（应收-收款，收款退款[包含正常单据关联和非单据关联的情况下的正常应收收款核销及收款退款核销]） \t \r\n\t\t\t\t         select AR.FID AS ARFId,PML.FTARGETBILLNO as RecBillNo,AR.FALLAMOUNTFOR,\r\n\t\t\t\t         CASE WHEN AR.FALLAMOUNTFOR < 0 \r\n\t\t\t\t         then -1 * abs(SUM(ISNULL(PML.FCURWRITTENOFFAMOUNTFOR,0))) \r\n\t\t\t\t         else abs(SUM(ISNULL(PML.FCURWRITTENOFFAMOUNTFOR,0))) end as FCURWRITTENOFFAMOUNTFOR,\r\n\t\t\t\t         case when AR.FWRITTENOFFSTATUS = 'C' then 0 else AR.FALLAMOUNTFOR - SUM(ISNULL(PML.FCURWRITTENOFFAMOUNTFOR,0)) end FNRECEIPTAMOUNT,\r\n                         0 AS FCHARGEOFFAMOUNT,N'' AS FCHARGEOFFBILLNO   \r\n\t\t\t\t           from T_AR_RECMACTHLOGENTRY PML \r\n\t\t\t\t         INNER JOIN  T_AR_RECMacthLog PM ON PM.FID = PML.FID And PM.FISACTIVATION<>'0'\r\n\t\t\t\t         INNER JOIN  T_AR_RECEIVABLE AR  ON AR.FID=PML.FSRCBILLID                          \r\n\t\t\t\t         WHERE  PML.FSOURCEFROMID='AR_receivable' AND (PML.FTARGETFROMID='AR_RECEIVEBILL'  OR PML.FTARGETFROMID='AR_REFUNDBILL')                        \r\n\t\t\t\t         and( \r\n                            exists(Select 1 from T_AR_RECEIVABLEENTRY ARE inner join {0} Flow on Flow.FRECID=ARE.FENTRYID WHERE ARE.FID=AR.FID \r\n                            union all select 1 from T_AR_BillingMatchLogENTRY blog inner join {0} Flow on Flow.FRECID=blog.FTARGETENTRYID\r\n                            inner join T_AR_RECMACTHLOGENTRY PML on blog.FSRCBillId=PML.FSRCBillId \r\n\t\t\t\t            where blog.FSOURCEFROMID='AR_receivable' and blog.FTARGETFROMID='AR_receivable')\r\n\t\t\t\t           )\r\n\t\t\t\t         group by AR.FID,PML.FTARGETBILLNO,AR.FALLAMOUNTFOR,AR.FWRITTENOFFSTATUS\r\n\t\t\t\t         Union All\r\n\t\t\t\t         --应收正 -  应收负的特殊核销\r\n\t\t\t\t\t         select AR.FID AS RecId,N'' as RecBillNo,AR.FALLAMOUNTFOR,\r\n                         0 as FCURWRITTENOFFAMOUNTFOR,\t\t\t\t    \r\n\t\t\t\t         0 AS FNRECEIPTAMOUNT, \r\n                         CASE WHEN AR.FALLAMOUNTFOR < 0 \r\n\t\t\t\t         then -1 * abs(SUM(ISNULL(PML.FCURWRITTENOFFAMOUNTFOR,0))) \r\n\t\t\t\t         else abs(SUM(ISNULL(PML.FCURWRITTENOFFAMOUNTFOR,0))) end as FCHARGEOFFAMOUNT,PML.FSRCBILLNO AS FCHARGEOFFBILLNO   \r\n\t\t\t\t           from T_AR_RECMACTHLOGENTRY PML \r\n\t\t\t\t         INNER JOIN  T_AR_RECMacthLog PM ON PM.FID = PML.FID And PM.FISACTIVATION<>'0'\r\n\t\t\t\t         INNER JOIN  T_AR_RECEIVABLE AR  ON AR.FID=PML.FTARGETBILLID                         \r\n\t\t\t\t         WHERE  PML.FTARGETFROMID='AR_receivable' AND PML.FSOURCEFROMID = 'AR_receivable'\r\n\t\t\t\t         and exists(Select 1 from T_AR_RECEIVABLEENTRY ARE inner join {0} Flow on Flow.FRECID=ARE.FENTRYID WHERE ARE.FID=AR.FID )\r\n\t\t\t\t         group by AR.FID,PML.FSRCBILLNO,AR.FALLAMOUNTFOR,AR.FWRITTENOFFSTATUS \t \r\n\t\t\t\t         Union All\r\n\t\t\t\t         --除了应收正 -  应收负的特殊核销[为了便于维护，此处不与上面（应收正，应收付核销条件）合并]\r\n  \t\t\t\t\t\t         select AR.FID AS RecId,N'' as RecBillNo,AR.FALLAMOUNTFOR,\r\n\t\t\t\t         0 as FCURWRITTENOFFAMOUNTFOR,\r\n\t\t\t\t         0 AS FNRECEIPTAMOUNT, \r\n\t\t                 CASE WHEN AR.FALLAMOUNTFOR < 0 \r\n\t\t\t\t         then -1 * abs(SUM(ISNULL(PML.FCURWRITTENOFFAMOUNTFOR,0))) \r\n\t\t\t\t         else abs(SUM(ISNULL(PML.FCURWRITTENOFFAMOUNTFOR,0))) end as FCHARGEOFFAMOUNT,PML.FSRCBILLNO AS FCHARGEOFFBILLNO  \r\n\t\t\t\t           from T_AR_RECMACTHLOGENTRY PML \r\n\t\t\t\t         INNER JOIN  T_AR_RECMacthLog PM ON PM.FID = PML.FID And PM.FISACTIVATION<>'0'\r\n\t\t\t\t         INNER JOIN  T_AR_RECEIVABLE AR  ON AR.FID=PML.FTARGETBILLID \r\n                         WHERE  PML.FTARGETFROMID='AR_receivable' AND PML.FSOURCEFROMID <> 'AR_receivable' AND PML.FSOURCEFROMID <> 'AR_RECEIVEBILL' AND PML.FSOURCEFROMID<>'AR_REFUNDBILL'\r\n\t\t\t\t         and exists(Select 1 from T_AR_RECEIVABLEENTRY ARE inner join {0} Flow on Flow.FRECID=ARE.FENTRYID WHERE ARE.FID=AR.FID )\r\n                        --预收款做特殊核销时，应收冲预收时不纳入冲销金额 \r\n                         AND Not Exists\r\n                        (SELECT 1 FROM T_AR_MATCKENTRY TARME INNER JOIN T_AR_RECMACTHLOGENTRY TARR \r\n                                ON TARR.FSRCROWID = TARME.FENTRYID  AND TARR.FSOURCETYPE = 'b9b2335770b84a3aa9b09b22767cd7e3'\r\n                                WHERE PML.FENTRYID=TARR.FENTRYID  AND TARME.FMATCHTYPE = '3' \r\n                         )\r\n\t\t\t\t         group by AR.FID,PML.FSRCBILLNO,AR.FALLAMOUNTFOR,AR.FWRITTENOFFSTATUS \t\r\n\t\t\t\t         Union All\r\n\t\t\t\t\t     --应付与应收匹配核销\r\n  \t\t\t\t\t\t         select AR.FID AS RecId,N'' as RecBillNo,AR.FALLAMOUNTFOR,\r\n\t\t\t\t         0 as FCURWRITTENOFFAMOUNTFOR,\r\n\t\t\t\t         0 AS FNRECEIPTAMOUNT,\r\n                         CASE WHEN AR.FALLAMOUNTFOR < 0 \r\n\t\t\t\t         then -1 * abs(SUM(ISNULL(PML.FCURWRITTENOFFAMOUNTFOR,0))) \r\n\t\t\t\t         else abs(SUM(ISNULL(PML.FCURWRITTENOFFAMOUNTFOR,0))) end as FCHARGEOFFAMOUNT,PML.FSRCBILLNO AS FCHARGEOFFBILLNO  \r\n\t\t\t\t           from T_AP_PAYMATCHLOGENTRY PML \r\n\t\t\t\t         INNER JOIN  T_AP_PAYMATCHLOG PM ON PM.FID = PML.FID And PM.FISACTIVATION<>'0'\r\n\t\t\t\t         INNER JOIN  T_AR_RECEIVABLE AR  ON AR.FID=PML.FTARGETBILLID \r\n\t\t\t\t         WHERE  PML.FTARGETFROMID='AR_receivable' \r\n\t\t\t\t         and exists(Select 1 from T_AR_RECEIVABLEENTRY ARE inner join {0} Flow on Flow.FRECID=ARE.FENTRYID WHERE ARE.FID=AR.FID )\r\n\t\t\t\t         group by AR.FID,PML.FSRCBILLNO,AR.FALLAMOUNTFOR,AR.FWRITTENOFFSTATUS \t\r\n                         --预收款处理\r\n                         Union All\r\n                          select ARM2.ARFID,ARM1.FSRCBILLNO as RecBillNo,ARM2.FALLAMOUNTFOR,\r\n                            sum(ISNULL(ARM1.FMATCHAMOUNTFOR,0)) as FCURWRITTENOFFAMOUNTFOR, \r\n                            0 AS FNRECEIPTAMOUNT, 0 as FCHARGEOFFAMOUNT,N'' as FCHARGEOFFBILLNO  \r\n                            from  T_AR_MatckEntry ARM1 inner join \r\n                            (select DISTINCT ARMat.FID,ARMat.FSRCBILLID as ARFID,ARMat.FSRCBILLNO as ARBillNO,ar.FALLAMOUNTFOR from T_AR_MatckEntry ARMat inner join  T_AR_RECEIVABLE ar \r\n                            on ARMat.FSRCBILLID=ar.FID and FMATCHTYPE=3 and FPURPOSEID=0) ARM2 on ARM1.FID=arm2.FID\r\n                            where ARM1.FPURPOSEID=20011 and FMATCHTYPE=3    \r\n                             and exists(Select 1 from T_AR_RECEIVABLEENTRY ARE inner join {0} Flow on Flow.FRECID=ARE.FENTRYID WHERE ARE.FID=ARM2.ARFID )\r\n                            group by ARM2.ARFID,ARM1.FSRCBILLNO,ARM2.FALLAMOUNTFOR\r\n                        --追加最新一种场景，预收款1200，订单1200，应收1000，开票1200，应收-开票核销1000，产生200应收调整单，应收调整单200与预收款1200核销200 at 20170314 by CXF\r\n                        UNION ALL\r\n                        SELECT \r\n                        AR.FID as ARFID,\r\n                                N'' RECBILLNO,\r\n                                AR.FALLAMOUNTFOR,\r\n                                0 FCURWRITTENOFFAMOUNTFOR,\r\n                                0 FNRECEIPTAMOUNT,\r\n                                SUM(ISNULL(RME.FCURWRITTENOFFAMOUNTFOR, 0)) FCHARGEOFFAMOUNT,\r\n                                RME.FSRCBillNo FCHARGEOFFBILLNO                 \r\n                        FROM T_AR_BILLINGMATCHLOGENTRY BME  \r\n                        inner join t_ar_receivablePlan rcp on rcp.FId=BME.FSrcBillId   \r\n                        INNER JOIN T_AR_RECMACTHLOGENTRY RME  on rcp.FEntryId=RME.FTargetEntryId                        \r\n                        INNER JOIN T_AR_RECEIVABLE AR on AR.FID=BME.FTargetBillId \r\n                        INNER JOIN {0} Flow on Flow.FRecid=BME.FTargetEntryId \r\n                        Where BME.FTargetFromid='AR_receivable' AND BME.FIsadiBill='1' and RME.Ftargetfromid='AR_receivable' AND RME.Fisadibill ='1'\r\n                                    GROUP BY AR.FID,\r\n                                RME.FSRCBILLNO,\r\n                                AR.FALLAMOUNTFOR,\r\n                                AR.FWRITTENOFFSTATUS\r\n\t\t\t\t        ) TMatch Group by TMatch.ARFId,TMatch.RecBillNo,TMatch.FALLAMOUNTFOR,FCHARGEOFFBILLNO", flowTable));
        list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
        stringBuilder.Clear();
        stringBuilder.AppendLine(string.Format(" INSERT /*+append*/ INTO {0}", receivableTable));
        stringBuilder.AppendLine(" (");
        stringBuilder.AppendLine(" FID,FRECBILLNO,FRECID,FALLAMOUNTFOR,FRECEIVEAMOUNT,FNRECEIPTAMOUNT,FROWAMOUNTFOR,FCHARGEOFFBILLNO,FCHARGEOFFAMOUNT");
        stringBuilder.AppendLine(" ,FSEQ ");
        stringBuilder.AppendLine(" )");
        stringBuilder.AppendFormat("\r\nselect UA.FID,UA.FRECBILLNO,UA.FRECID,UA.FALLAMOUNTFOR,UA.FRECEIVEAMOUNT,\r\nUA.FNRECEIPTAMOUNT,UA.FROWAMOUNTFOR,UA.FCHARGEOFFBILLNO,UA.FCHARGEOFFAMOUNT,ROW_NUMBER() OVER (ORDER BY UA.FRECID  ASC )\r\nfrom (\r\n\r\nselect ARE.FID,MM.FRECBILLNO,ARE.FENTRYID as FRECID,ARERowGroup.RealRowAmount as FALLAMOUNTFOR,\r\n                ARERowGroup.RealRowAmount/MM.FALLAMOUNTFOR*MMGroup.FRECEIVEAMOUNT as FRECEIVEAMOUNT,\r\n                ARERowGroup.RealRowAmount/MM.FALLAMOUNTFOR*MMGroup.FNRECEIPTAMOUNT as FNRECEIPTAMOUNT, \r\n \t            ARE.FALLAMOUNTFOR as FROWAMOUNTFOR,MM.FCHARGEOFFBILLNO,\r\n                ARERowGroup.RealRowAmount/MM.FALLAMOUNTFOR*MMGroup.FCHARGEOFFAMOUNT as FCHARGEOFFAMOUNT               \r\n \t          from {0} MM \r\n              inner join (select ARE.FID,SUM(ARE.FALLAMOUNTFOR) as RealRowAmount FROM T_AR_RECEIVABLEENTRY ARE \r\n \t                      inner join {1} flow on flow.frecid=are.FENTRYID  group by ARE.fid ) ARERowGroup  on MM.FID=ARERowGroup.FID\r\n\t          inner join (select MM.FID,SUM(FRECEIVEAMOUNT) as FRECEIVEAMOUNT,SUM(FNRECEIPTAMOUNT) as FNRECEIPTAMOUNT,SUM(FCHARGEOFFAMOUNT) as FCHARGEOFFAMOUNT\r\n \t                      from {0} MM group by MM.FID) MMGroup  on MM.FID=MMGroup.FID\r\n \t          inner join T_AR_RECEIVABLEENTRY ARE on ARE.FID=MM.FID\r\n         --追加一种复杂的情况，开票调整单单独下推收款单的处理\r\nUnion All\r\n            select TT.FID,TT.FRECBILLNO,TT.FRECID,TTSum.FALLAMOUNTFOR,TTSum.FRECEIVEAMOUNT,TT.FNRECEIPTAMOUNT,\r\n            TT.FROWAMOUNTFOR,TT.FCHARGEOFFBILLNO,TT.FNRECEIPTAMOUNT\r\n            from (\r\n                        select blog.FTARGETBILLID as FID,PML.FTARGETBILLNO as FRECBILLNO, blog.FTARGETENTRYID as FRECID,\r\n                        are.FALLAMOUNTFOR,pml.FCURWRITTENOFFAMOUNTFOR as FRECEIVEAMOUNT,0 as FNRECEIPTAMOUNT,\r\n                        are.FALLAMOUNTFOR as FROWAMOUNTFOR,N'' as FCHARGEOFFBILLNO,0 as FCHARGEOFFAMOUNT\r\n                        from   T_AR_RECMACTHLOGENTRY PML \r\n                        inner join  T_AR_BillingMatchLogENTRY blog on blog.FSRCBillId=PML.FSRCBillId \r\n                        inner join  {1} Flow on Flow.FRECID=blog.FTARGETENTRYID\r\n                        inner join T_AR_RECEIVABLEENTRY ARE on ARE.FENTRYID=blog.FSRCROWID\r\n                        where blog.FSOURCEFROMID='AR_receivable' and blog.FTARGETFROMID='AR_receivable'  \r\n                         and PML.FSOURCEFROMID='AR_receivable' AND (PML.FTARGETFROMID='AR_RECEIVEBILL'  OR PML.FTARGETFROMID='AR_REFUNDBILL') \r\n              ) TT\r\n             inner join\r\n             ( \r\n             \tselect blog.FTARGETBILLID as FID,\r\n\t\t\t\tsum(are.FALLAMOUNTFOR) as FALLAMOUNTFOR,sum(pml.FCURWRITTENOFFAMOUNTFOR) as FRECEIVEAMOUNT\t\t\t\t\r\n\t\t\t\tfrom   T_AR_RECMACTHLOGENTRY PML \r\n                inner join  T_AR_BillingMatchLogENTRY blog on blog.FSRCBillId=PML.FSRCBillId \r\n                inner join  {1} Flow on Flow.FRECID=blog.FTARGETENTRYID\r\n                inner join T_AR_RECEIVABLEENTRY ARE on ARE.FENTRYID=blog.FSRCROWID\r\n                where blog.FSOURCEFROMID='AR_receivable' and blog.FTARGETFROMID='AR_receivable'  \r\n                and PML.FSOURCEFROMID='AR_receivable' AND (PML.FTARGETFROMID='AR_RECEIVEBILL'  OR PML.FTARGETFROMID='AR_REFUNDBILL') \r\n\t\t\t\tgroup by  blog.FTARGETBILLID\r\n             ) TTSum on TT.FID=TTSum.FID\r\n) UA\r\n", receivableHelpTable, flowTable);
        list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
        stringBuilder.Clear();
        stringBuilder.AppendLine(string.Format("/*dialect*/ MERGE INTO {0}  P USING\r\n            (\r\n            SELECT REC.FSEQ,REC.FID,SA.FALLAMOUNTFOR,SA.FRECEIVEAMOUNT,SA.FCHARGEOFFAMOUNT FROM {0} REC \r\n            INNER JOIN (\r\n                        SELECT FID, SUM(FALLAMOUNTFOR) as FALLAMOUNTFOR,SUM(FRECEIVEAMOUNT) as FRECEIVEAMOUNT,SUM(FCHARGEOFFAMOUNT) AS FCHARGEOFFAMOUNT  \r\n                        FROM (SELECT DISTINCT FID, FALLAMOUNTFOR,FRECEIVEAMOUNT,FCHARGEOFFAMOUNT FROM {0}) ST GROUP BY FID\r\n                        ) SA ON rec.FID=SA.FID\r\n            ) UT\r\n            ON (UT.FSEQ=P.FSEQ and UT.FID=P.FID  )\r\n            WHEN MATCHED THEN UPDATE SET P.FALLAMOUNTFOR=UT.FALLAMOUNTFOR,P.FRECEIVEAMOUNT=UT.FRECEIVEAMOUNT,P.FCHARGEOFFAMOUNT=UT.FCHARGEOFFAMOUNT {1}", receivableTable, (ctx.DatabaseType == DatabaseType.MS_SQL_Server) ? ";" : ""));
        list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
        stringBuilder.Clear();
        stringBuilder.AppendLine(string.Format("/*dialect*/ MERGE INTO {0}  P USING\r\n            (\r\n            SELECT REC.FSEQ,REC.FRECID,SA.FROWAMOUNTFOR FROM {0} REC \r\n            INNER JOIN (\r\n                SELECT FRECID, SUM(FROWAMOUNTFOR) as FROWAMOUNTFOR  FROM (SELECT distinct FRECID, FROWAMOUNTFOR FROM {0}) ST GROUP BY FRECID\r\n                ) SA on REC.FRECID=SA.FRECID\r\n            ) UT\r\n            ON (UT.FSEQ=P.FSEQ and UT.FRECID=P.FRECID  )\r\n            WHEN MATCHED THEN UPDATE SET P.FROWAMOUNTFOR=UT.FROWAMOUNTFOR {1}", receivableTable, (ctx.DatabaseType == DatabaseType.MS_SQL_Server) ? ";" : ""));
        list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
        stringBuilder.Clear();
        DBUtils.ExecuteBatchWithTime(ctx, list, 300);
        stringBuilder.AppendFormat(" IF NOT EXISTS (SELECT 1 FROM KSQL_INDEXES WHERE KSQL_INDNAME = 'idx_{0}_1') CREATE INDEX idx_{0}_1 ON {1} (FRECID)", receivableTable.Substring(3, 22), receivableTable);
        DBUtils.Execute(ctx, stringBuilder.ToString());
    }
    public static void GetReceBillNo(Context ctx, string receivableTable, string receBillnoTable)
    {
        new List<SqlObject>();
        string strSQL = string.Format("select FRECID,FRECBILLNO,FCHARGEOFFBILLNO from {0} order by FSeq ASC ", receivableTable);
        ServiceHelper.GetService<IQueryService>();
        DynamicObjectCollection dynamicObjectCollection = DBUtils.ExecuteDynamicObject(ctx, strSQL, null, null, CommandType.Text, new SqlParam[0]);
        Dictionary<long, string> dictionary = new Dictionary<long, string>();
        Dictionary<long, string> dictionary2 = new Dictionary<long, string>();
        StringBuilder stringBuilder = new StringBuilder();
        List<string> list = new List<string>();
        List<string> list2 = new List<string>();
        List<long> list3 = new List<long>();
        string text = string.Empty;
        string text2 = string.Empty;
        if (dynamicObjectCollection.Count <= 0)
        {
            return;
        }
        long num = 0L;
        DynamicObject dynamicObject = dynamicObjectCollection[0];
        if (dynamicObject != null)
        {
            num = Convert.ToInt64(dynamicObject["FRECID"]);
            text = Convert.ToString(dynamicObject["FRECBILLNO"]);
            text2 = Convert.ToString(dynamicObject["FCHARGEOFFBILLNO"]);
            if (!string.IsNullOrWhiteSpace(text) && !list.Contains(text))
            {
                list.Add(text);
            }
            if (!string.IsNullOrWhiteSpace(text2) && !list2.Contains(text2))
            {
                list2.Add(text2);
            }
            if (!list3.Contains(num))
            {
                list3.Add(num);
            }
        }
        for (int i = 1; i < dynamicObjectCollection.Count; i++)
        {
            dynamicObject = dynamicObjectCollection[i];
            if (dynamicObject != null)
            {
                if (num == Convert.ToInt64(dynamicObject["FRECID"]))
                {
                    text = Convert.ToString(dynamicObject["FRECBILLNO"]);
                    text2 = Convert.ToString(dynamicObject["FCHARGEOFFBILLNO"]);
                    if (!string.IsNullOrWhiteSpace(text) && !list.Contains(text))
                    {
                        list.Add(text);
                    }
                    if (!string.IsNullOrWhiteSpace(text2) && !list2.Contains(text2))
                    {
                        list2.Add(text2);
                    }
                    if (!list3.Contains(num))
                    {
                        list3.Add(num);
                    }
                }
                else
                {
                    if (list.Count > 0)
                    {
                        if (list.Count > 20)
                        {
                            list.RemoveRange(20, list.Count - 20);
                            list.Add("......");
                        }
                        dictionary.Add(num, string.Join(",", list.ToArray()));
                    }
                    if (list2.Count > 0)
                    {
                        if (list2.Count > 20)
                        {
                            list2.RemoveRange(20, list2.Count - 20);
                            list2.Add("......");
                        }
                        dictionary2.Add(num, string.Join(",", list2.ToArray()));
                    }
                    num = Convert.ToInt64(dynamicObject["FRECID"]);
                    list.Clear();
                    list2.Clear();
                    text = Convert.ToString(dynamicObject["FRECBILLNO"]);
                    text2 = Convert.ToString(dynamicObject["FCHARGEOFFBILLNO"]);
                    if (!string.IsNullOrWhiteSpace(text) && !list.Contains(text))
                    {
                        list.Add(text);
                    }
                    if (!string.IsNullOrWhiteSpace(text2) && !list2.Contains(text2))
                    {
                        list2.Add(text2);
                    }
                    if (!list3.Contains(num))
                    {
                        list3.Add(num);
                    }
                }
            }
        }
        if (list.Count > 0)
        {
            if (list.Count > 20)
            {
                list.RemoveRange(20, list.Count - 20);
                list.Add("......");
            }
            dictionary.Add(num, string.Join(",", list.ToArray()));
        }
        if (list2.Count > 0)
        {
            if (list2.Count > 20)
            {
                list2.RemoveRange(20, list2.Count - 20);
                list2.Add("......");
            }
            dictionary2.Add(num, string.Join(",", list2.ToArray()));
        }
        num = Convert.ToInt64(dynamicObject["FRECID"]);
        list.Clear();
        list2.Clear();
        if (list3 != null && list3.Count > 0)
        {
            stringBuilder.Clear();
            DataTable dataTable = new DataTable(receBillnoTable);
            dataTable.Columns.Add("FRECID", typeof(long));
            dataTable.Columns.Add("FRECBILLNO", typeof(string));
            dataTable.Columns.Add("FCHARGEOFFBILLNO", typeof(string));
            foreach (long current in list3)
            {
                DataRow dataRow = dataTable.NewRow();
                dataRow["FRECID"] = current;
                dataRow["FRECBILLNO"] = (dictionary.ContainsKey(current) ? dictionary[current] : "");
                dataRow["FCHARGEOFFBILLNO"] = (dictionary2.ContainsKey(current) ? dictionary2[current] : "");
                dataTable.Rows.Add(dataRow);
            }
            DBUtils.BulkInserts(ctx, dataTable);
        }
    }
    public static void GetInvoceBillNo(Context ctx, string invoceTable, string invoceBillnoTable)
    {
        new List<SqlObject>();
        string strSQL = string.Format("select FRECID,FINVOCEBILLNO from {0} order by FSeq ASC", invoceTable);
        ServiceHelper.GetService<IQueryService>();
        DynamicObjectCollection dynamicObjectCollection = DBUtils.ExecuteDynamicObject(ctx, strSQL, null, null, CommandType.Text, new SqlParam[0]);
        Dictionary<long, string> dictionary = new Dictionary<long, string>();
        StringBuilder stringBuilder = new StringBuilder();
        List<string> list = new List<string>();
        string text = string.Empty;
        if (dynamicObjectCollection.Count <= 0)
        {
            return;
        }
        long num = 0L;
        DynamicObject dynamicObject = dynamicObjectCollection[0];
        if (dynamicObject != null)
        {
            num = Convert.ToInt64(dynamicObject["FRECID"]);
            text = Convert.ToString(dynamicObject["FINVOCEBILLNO"]);
            if (!string.IsNullOrWhiteSpace(text) && !list.Contains(text))
            {
                list.Add(text);
            }
        }
        for (int i = 1; i < dynamicObjectCollection.Count; i++)
        {
            dynamicObject = dynamicObjectCollection[i];
            if (dynamicObject != null)
            {
                if (num == Convert.ToInt64(dynamicObject["FRECID"]))
                {
                    text = Convert.ToString(dynamicObject["FINVOCEBILLNO"]);
                    if (!string.IsNullOrWhiteSpace(text) && !list.Contains(text))
                    {
                        list.Add(text);
                    }
                }
                else
                {
                    if (list.Count > 0)
                    {
                        dictionary.Add(num, string.Join(",", list.ToArray()));
                    }
                    num = Convert.ToInt64(dynamicObject["FRECID"]);
                    list.Clear();
                    text = Convert.ToString(dynamicObject["FINVOCEBILLNO"]);
                    if (!string.IsNullOrWhiteSpace(text) && !list.Contains(text))
                    {
                        list.Add(text);
                    }
                }
            }
        }
        if (list.Count > 0)
        {
            dictionary.Add(num, string.Join(",", list.ToArray()));
        }
        num = Convert.ToInt64(dynamicObject["FRECID"]);
        list.Clear();
        if (dictionary != null)
        {
            stringBuilder.Clear();
            DataTable dataTable = new DataTable(invoceBillnoTable);
            dataTable.Columns.Add("FRECID", typeof(long));
            dataTable.Columns.Add("FINVOCEBILLNO", typeof(string));
            foreach (KeyValuePair<long, string> current in dictionary)
            {
                DataRow dataRow = dataTable.NewRow();
                dataRow["FRECID"] = current.Key;
                dataRow["FINVOCEBILLNO"] = current.Value;
                dataTable.Rows.Add(dataRow);
            }
            DBUtils.BulkInserts(ctx, dataTable);
        }
    }
    public static void CreateRecBillnoTable(string receBillnoTable, List<SqlObject> lstTable)
    {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.AppendLine(string.Format(" CREATE TABLE {0}", receBillnoTable));
        stringBuilder.AppendLine("(");
        stringBuilder.AppendLine("  FRECID INT NULL ");
        stringBuilder.AppendLine("  ,FRECBILLNO  nvarchar(1000) NULL ");
        stringBuilder.AppendLine("  ,FCHARGEOFFBILLNO  nvarchar(1000) NULL ");
        stringBuilder.AppendLine(" )");
        lstTable.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
        stringBuilder.Clear();
    }
    public static void CreateInvoceBillnoTable(string invoceBillnoTable, List<SqlObject> lstTable)
    {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.AppendLine(string.Format(" CREATE TABLE {0}", invoceBillnoTable));
        stringBuilder.AppendLine("(");
        stringBuilder.AppendLine("  FRECID INT NULL ");
        stringBuilder.AppendLine("  ,FINVOCEBILLNO  nvarchar(1000) NULL ");
        stringBuilder.AppendLine(" )");
        lstTable.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
        stringBuilder.Clear();
    }
    internal static void GetNeedDataByFields(Context ctx, List<string> lstTable, string flowData, string flowDataTable, long[] entryIds)
    {
        StringBuilder stringBuilder = new StringBuilder();
        StringBuilder stringBuilder2 = new StringBuilder();
        List<SqlObject> list = new List<SqlObject>();
        //关联源单，和目标单
        foreach (string current in lstTable)
        {
            stringBuilder.Append(string.Format("{0},{0}_SID,", current));
        }
        stringBuilder.Append(" FIDENTITYID");
        //拆分字段
        string[] array = stringBuilder.ToString().Split(new char[]
        {
                ','
        });
        stringBuilder2.AppendLine(string.Format(" CREATE TABLE {0} ( ", flowDataTable));
        List<string> list2 = new List<string>();
        string[] array2 = array;
        for (int i = 0; i < array2.Length; i++)
        {
            string arg = array2[i];
            list2.Add(string.Format(" {0} INT NULL ", arg));
        }
        //拼接流程明细表
        string arg2 = string.Join(",", list2.ToArray());
        stringBuilder2.AppendLine(string.Format(" {0} )", arg2));

        list.Add(new SqlObject(stringBuilder2.ToString(), new List<SqlParam>()));
        stringBuilder2.Clear();
        //生成流程数据
        stringBuilder2.AppendLine(string.Format(" INSERT INTO {0} ({1}) SELECT {2} FROM {3} T1", new object[]
        {
                flowDataTable,
                stringBuilder.ToString(),
                stringBuilder.ToString(),
                flowData
        }));
        stringBuilder2.AppendLine(" WHERE EXISTS (SELECT 1 FROM TABLE(fn_StrSplit(@FID, ',',1)) B WHERE B.FID=T1.T_SAL_ORDERENTRY) ");
        SqlParam item = new SqlParam("@FID", KDDbType.udt_inttable, entryIds);
        list.Add(new SqlObject(stringBuilder2.ToString(), new List<SqlParam>
            {
                item
            }));
        stringBuilder.Clear();
        stringBuilder2.Clear();
        //删除组织结算生成的
        stringBuilder2.AppendLine(string.Format(" DELETE FROM {0}\r\n                                WHERE EXISTS(\r\n                                SELECT 1 FROM T_SAL_OUTSTOCKENTRY OE INNER JOIN T_SAL_OUTSTOCKFIN OHF ON OHF.FID=OE.FID \r\n                                WHERE OHF.FISGENFORIOS='1' AND {0}.T_Sal_OUTSTOCKENTRY=OE.FENTRYID ) ", flowDataTable));
        list.Add(new SqlObject(stringBuilder2.ToString(), new List<SqlParam>()));
        stringBuilder2.Clear();
        //stringBuilder2.AppendLine(string.Format(" DELETE FROM {0} \r\n                                WHERE EXISTS(\r\n                                SELECT 1 FROM T_SAL_RETURNSTOCKENTRY ORE INNER JOIN T_SAL_RETURNSTOCKFIN ORF ON ORF.FID=ORE.FID \r\n                                WHERE ORF.FISGENFORIOS='1' AND {0}.T_Sal_RETURNSTOCKENTRY=ORE.FENTRYID ) ", flowDataTable));
        //list.Add(new SqlObject(stringBuilder2.ToString(), new List<SqlParam>()));
        //stringBuilder2.Clear();
        DBUtils.ExecuteBatchWithTime(ctx, list, 300);
    }
    public static string AddTempTable(Context ctx)
    {
        IDBService service = ServiceHelper.GetService<IDBService>();
        return service.CreateTemporaryTableName(ctx);
    }
    public static void DropTempTables(Context ctx, List<string> deleteTables)
    {
        ITemporaryTableService service = ServiceHelper.GetService<ITemporaryTableService>();
        service.DropTable(ctx, new HashSet<string>(deleteTables));
    }
    public static string GetfilterGroupDataIsolation(Context ctx, string orgList, BusinessGroupDataIsolationArgs isolationArgs)
    {
        ICommonService commonService = Kingdee.K3.SCM.Contracts.ServiceFactory.GetCommonService(ctx);
        string text = string.Empty;
        List<long> list = new List<long>();
        List<long> list2 = new List<long>();
        string[] array = orgList.Split(new char[]
        {
                ','
        });
        string[] array2 = array;
        for (int i = 0; i < array2.Length; i++)
        {
            string value = array2[i];
            if (!value.IsNullOrEmptyOrWhiteSpace())
            {
                object systemProfile = commonService.GetSystemProfile(ctx, Convert.ToInt64(value), isolationArgs.PurchaseParameterObject, isolationArgs.PurchaseParameterKey, false);
                if (Convert.ToBoolean(systemProfile))
                {
                    list.Add(Convert.ToInt64(value));
                }
                else
                {
                    list2.Add(Convert.ToInt64(value));
                }
            }
        }
        if (list != null && list.Count > 0)
        {
            string operatorGroupIds = commonService.GetOperatorGroupIds(ctx, ctx.UserId, isolationArgs.OperatorType);
            if (!operatorGroupIds.IsNullOrEmptyOrWhiteSpace())
            {
                if ((list != null && list.Count > 0) || orgList.IsNullOrEmptyOrWhiteSpace())
                {
                    text = string.Format("  ({0} IN ({1}) AND {2} IN({3}))", new object[]
                    {
                            isolationArgs.BusinessGroupKey,
                            operatorGroupIds,
                            isolationArgs.OrgIdKey,
                            string.Join<long>(",", list.ToArray())
                    });
                }
                if (list2 != null && list2.Count > 0 && !text.IsNullOrEmptyOrWhiteSpace() && list2 != null && list2.Count > 0)
                {
                    text += string.Format(" OR {0}  IN ({1}) ", isolationArgs.OrgIdKey, string.Join<long>(",", list2.ToArray()));
                }
            }
        }
        return text;
    }
    public static Dictionary<string, string> GetFlexValues(Context ctx, int type = 0)
    {
        Dictionary<string, string> dictionary = new Dictionary<string, string>();
        string strSQL = (type == 0) ? string.Format(" SELECT FFLEXNUMBER AS FKey,LFL.FNAME AS FlexValue\r\n                                FROM T_BAS_FLEXVALUES LF\r\n                                LEFT JOIN T_BAS_FLEXVALUES_L LFL ON LF.FID=LFL.FID \r\n                                AND LFL.FLOCALEID = {0}\r\n                                WHERE FDOCUMENTSTATUS = 'C' AND FFLEXNUMBER <> ' ' ORDER BY LF.FID ", ctx.UserLocale.LCID) : string.Format(" SELECT FFLEXNUMBER AS FKey,LFL.FNAME AS FlexValue\r\n                                FROM T_BD_FLEXAUXPROPERTY LF\r\n                                LEFT JOIN T_BD_FLEXAUXPROPERTY_L LFL ON LF.FID=LFL.FID \r\n                                AND LFL.FLOCALEID = {0}\r\n                                WHERE FDOCUMENTSTATUS = 'C' AND FFLEXNUMBER <> ' ' ORDER BY LF.FID ", ctx.UserLocale.LCID);
        using (IDataReader dataReader = DBUtils.ExecuteReader(ctx, strSQL))
        {
            while (dataReader.Read())
            {
                dictionary.Add(Convert.ToString(dataReader["FKey"]), Convert.ToString(dataReader["FlexValue"]));
            }
            dataReader.Close();
        }
        return dictionary;
    }
    public static List<string> GetAuxPropExtValues(Context ctx, FormMetadata flexMetadata, IViewService iserver, ref Dictionary<string, string> auxPropValues, ref Dictionary<long, DynamicObject> auxPropDatas, long axupropID)
    {
        if (auxPropValues == null)
        {
            auxPropValues = SalRptCommon.GetFlexValues(ctx, 1);
        }
        if (flexMetadata == null)
        {
            IMetaDataService service = Kingdee.BOS.Contracts.ServiceFactory.GetService<IMetaDataService>(ctx);
            flexMetadata = (FormMetadata)service.Load(ctx, "BD_FLEXSITEMDETAILV", true);
            iserver = Kingdee.BOS.Contracts.ServiceFactory.GetService<IViewService>(ctx);
        }
        List<string> list = new List<string>();
        if (axupropID > 0L && auxPropValues.Count > 0)
        {
            DynamicObject dynamicObject = null;
            auxPropDatas.TryGetValue(axupropID, out dynamicObject);
            if (dynamicObject == null)
            {
                dynamicObject = iserver.LoadSingle(ctx, axupropID, flexMetadata.BusinessInfo.GetDynamicObjectType());
                auxPropDatas[axupropID] = dynamicObject;
            }
            using (Dictionary<string, string>.KeyCollection.Enumerator enumerator = auxPropValues.Keys.GetEnumerator())
            {
                while (enumerator.MoveNext())
                {
                    string current = enumerator.Current;
                    string text = current.Substring(1, current.Length - 1);
                    if (dynamicObject.DynamicObjectType.Properties.ContainsKey(text) && dynamicObject[text] != null)
                    {
                        if (dynamicObject[text] is DynamicObject)
                        {
                            if (((DynamicObject)dynamicObject[text]).DynamicObjectType.Properties.ContainsKey("Name"))
                            {
                                list.Add(auxPropValues[current] + ":" + Convert.ToString(((DynamicObject)dynamicObject[text])["Name"]));
                            }
                            else
                            {
                                list.Add(auxPropValues[current] + ":" + Convert.ToString(((DynamicObject)dynamicObject[text])["FDataValue"]));
                            }
                        }
                        else
                        {
                            if (dynamicObject[text] is bool)
                            {
                                list.Add(auxPropValues[current] + ":" + (Convert.ToBoolean(dynamicObject[text]) ? ResManager.LoadKDString("是", "004102030005356", SubSystemType.SCM, new object[0]) : ResManager.LoadKDString("否", "004102030005359", SubSystemType.SCM, new object[0])));
                            }
                            else
                            {
                                if (!string.IsNullOrWhiteSpace(Convert.ToString(dynamicObject[text])))
                                {
                                    list.Add(auxPropValues[current] + ":" + Convert.ToString(dynamicObject[text]));
                                }
                            }
                        }
                    }
                }
                return list;
            }
        }
        list.Add("");
        return list;
    }
    public static List<BaseDataTempTable> GetBaseDataTempTable(Context ctx, string orgFilter, string formId)
    {
        List<BaseDataTempTable> result = null;
        if (!orgFilter.IsNullOrEmptyOrWhiteSpace())
        {
            List<long> orgIdList = new List<long>();
            List<string> list = orgFilter.Split(new char[]
            {
                    ','
            }).ToList<string>();
            list.ForEach(delegate (string x)
            {
                orgIdList.Add(Convert.ToInt64(x));
            });
            if (orgIdList.Count > 0)
            {
                IPermissionService permissionService = Kingdee.BOS.Contracts.ServiceFactory.GetPermissionService(ctx);
                result = permissionService.GetBaseDataTempTable(ctx, formId, orgIdList);
            }
        }
        return result;
    }
}
}
