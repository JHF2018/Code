using Kingdee.BOS;
using Kingdee.BOS.App;
using Kingdee.BOS.App.Core.BusinessFlow;
using Kingdee.BOS.App.Core.DataBase;
using Kingdee.BOS.App.Data;
using Kingdee.BOS.BusinessEntity.BusinessFlow;
using Kingdee.BOS.Contracts;
using Kingdee.BOS.Contracts.Report;
using Kingdee.BOS.Core.BusinessFlow.ServiceArgs;
using Kingdee.BOS.Core.CommonFilter;
using Kingdee.BOS.Core.Enums;
using Kingdee.BOS.Core.Metadata;
using Kingdee.BOS.Core.Permission;
using Kingdee.BOS.Core.Permission.Objects;
using Kingdee.BOS.Core.Report;
using Kingdee.BOS.Core.SqlBuilder;
using Kingdee.BOS.Core.Util;
using Kingdee.BOS.Orm.DataEntity;
using Kingdee.BOS.Resource;
using Kingdee.BOS.Util;
using Kingdee.K3.BD.Contracts;
using Kingdee.K3.Core.BD;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Transactions;
namespace JHF.Sal.App.Report
{
    public class SalProductDetailRpt : SysReportBaseService
    {
        private string tmpRptTbl = string.Empty;
        private string tempFilter = string.Empty;
        private string noCalData = string.Empty;
        private List<string> CreatInsertData = new List<string>();
        private Dictionary<string, string> auxPropValues;
        private IViewService iserver;
        private FormMetadata flexMetadata;
        private Dictionary<long, DynamicObject> auxPropDatas = new Dictionary<long, DynamicObject>();
        private List<DecimalControlField> lstDcf = new List<DecimalControlField>();
        private Dictionary<string, SqlStorageType> hidenColumns;
        public string filterOrgList = string.Empty;
        public int filterOrg;
        public int filterMoneyType;
        public DateTime filterSoDayFrom;
        public DateTime filterSoDayTo;
        public string filterCustomerFrom = string.Empty;
        public string filterCustomerTo = string.Empty;
        public string filterSalesFrom = string.Empty;
        public string filterSalesTo = string.Empty;
        public string filterMaterialFrom = string.Empty;
        public string filterMaterialTo = string.Empty;
        public string filterSFrom = string.Empty;
        public string filterSTo = string.Empty;
        public DateTime filterDeDayFrom;
        public DateTime filterDeDayTo;
        public int filterPriceFrom;
        public string filterFormStatus = string.Empty;
        public string filterFCStatus = string.Empty;
        public string filterBCStatus = string.Empty;
        public bool bMergingSOHeader = true;
        public bool bIncludedUnfilledOrders;
        private string priceFrom = string.Empty;
        public string superioreFilter = string.Empty;
        public string fEntryid = string.Empty;
        private bool isRecWithMat;
        private List<string> deleteTables = new List<string>();
        private string flowTable = string.Empty;
        private string filterSql = string.Empty;
        private string rptFilterTable = string.Empty;
        private string moreFilter = string.Empty;
        //private string receivableHelpTable = string.Empty;
        //private string receivableTable = string.Empty;
        //private string receBillnoTable = string.Empty;
        private string flowDataTable = string.Empty;
        //private string invoceTable = string.Empty;
        //private string invoceBillnoTable = string.Empty;
        //private string invoceBillno_RTable = string.Empty;
        //private string recAmountTable = string.Empty;
        //private string recSeqTable = string.Empty;
        protected bool onTime = true;
        public override void Initialize()
        {
            base.Initialize();
            base.ReportProperty.IdentityFieldName = "FIDENTITYID";
            base.ReportProperty.ReportType = ReportType.REPORTTYPE_NORMAL;
            base.ReportProperty.ReportName = new LocaleValue("销售生产明细表");
            base.ReportProperty.IsGroupSummary = true;
            base.ReportProperty.IsUIDesignerColumns = true;
            base.ReportProperty.PrimaryKeyFieldName = "FBILLNO";
            base.ReportProperty.DspInsteadColumnsInfo.DefaultDspInsteadColumns.Add("FSALEORGID", "FSALEORGNAME");
            base.ReportProperty.DspInsteadColumnsInfo.DefaultDspInsteadColumns.Add("FSALEDEPTID", "FSALEDEPT");
            base.ReportProperty.DspInsteadColumnsInfo.DefaultDspInsteadColumns.Add("FSALEGROUPID", "FSALEGROUP");
            base.ReportProperty.DspInsteadColumnsInfo.DefaultDspInsteadColumns.Add("FSALERID", "FSALES");
            base.ReportProperty.DspInsteadColumnsInfo.DefaultDspInsteadColumns.Add("FMATERIALID", "FMATERIALNUMBER");
            base.ReportProperty.DspInsteadColumnsInfo.DefaultDspInsteadColumns.Add("FMAPID", "FMAPNUMBER");
            base.ReportProperty.DspInsteadColumnsInfo.DefaultDspInsteadColumns.Add("FCURRENCYID", "FMONEYTYPE");
            base.ReportProperty.DspInsteadColumnsInfo.DefaultDspInsteadColumns.Add("FCUSTID", "FCUSTOMERNUMBER");
            base.ReportProperty.DspInsteadColumnsInfo.DefaultDspInsteadColumns.Add("FORDERID", "FORDERNAME");
            base.ReportProperty.DspInsteadColumnsInfo.DefaultDspInsteadColumns.Add("FMATERIALGROUP", "FMATERIALGROUPNAME");
            base.ReportProperty.DspInsteadColumnsInfo.DefaultDspInsteadColumns.Add("FAUXPROPID", "FAUXPROP");
            this.SetDecimalControl();
        }
        protected override DataTable GetReportData(string tablename, IRptParams filter)
        {
            DataTable reportData = base.GetReportData(tablename, filter);
            return this.GetThisRptPageData(reportData);
        }
        private DataTable GetThisRptPageData(DataTable dtPageData)
        {
            for (int i = 0; i < dtPageData.Rows.Count; i++)
            {
                if (dtPageData.Rows[i]["FAUXPROPID"] != DBNull.Value && Convert.ToInt64(dtPageData.Rows[i]["FAUXPROPID"]) > 0L)
                {
                    dtPageData.Rows[i]["FAUXPROP"] = string.Join(";", SalRptCommon.GetAuxPropExtValues(base.Context, this.flexMetadata, this.iserver, ref this.auxPropValues, ref this.auxPropDatas, Convert.ToInt64(dtPageData.Rows[i]["FAUXPROPID"])));
                }
            }
            dtPageData.AcceptChanges();
            return dtPageData;
        }
        public override void CloseReport()
        {
            if (this.deleteTables.Count > 0)
            {
                SalRptCommon.DropTempTables(base.Context, this.deleteTables);
            }
        }
        public override void BuilderReportSqlAndTempTable(IRptParams filter, string tableName)
        {
            this.SetFilter(filter);
            this.ModifyShowHideColumns(ref filter);
            this.SetHidenColumns(ref filter);
            this.InitTables();
            DynamicObject customFilter = filter.FilterParameter.CustomFilter;
            DynamicObject parameterData = filter.ParameterData;
            if (parameterData != null)
            {
                this.onTime = Convert.ToBoolean(parameterData["onTime"]);
            }
            this.moreFilter = filter.FilterParameter.FilterString.Trim();
            if (customFilter["SaleOrgList"] == null || (string.IsNullOrWhiteSpace(customFilter["SaleOrgList"].ToString()) && filter.CustomParams.Count <= 1))
            {
                this.SetRptBosTable(tableName);
                return;
            }
            string sQLWhere = this.GetSQLWhere();
            string text = this.filterOrgList;
            if (text.IsNullOrEmptyOrWhiteSpace())
            {
                Dictionary<string, object> dictionary = filter.CustomParams["ParentReportCurrentRow"] as Dictionary<string, object>;
                text = Convert.ToString(dictionary["FSALEORGID"]);
            }
            List<BaseDataTempTable> baseDataTempTable = filter.BaseDataTempTable;
            if (!text.IsNullOrEmptyOrWhiteSpace())
            {
                baseDataTempTable = SalRptCommon.GetBaseDataTempTable(base.Context, text, "SAL_DetailReport");
            }
            SalRptCommon.GetFilterOrder(base.Context, this.rptFilterTable, !string.IsNullOrWhiteSpace(this.moreFilter), sQLWhere, text, baseDataTempTable, "SAL_DetailReport");
            foreach (BaseDataTempTable current in baseDataTempTable)
            {
                this.deleteTables.Add(current.TempTable);
            }
            //从bf_instance表获取流程明细数据
            this.GetDataSource(this.rptFilterTable);
            //SalRptCommon.GetReceBillNo(base.Context, this.receivableTable, this.receBillnoTable);
            //SalRptCommon.GetInvoceBillNo(base.Context, this.invoceTable, this.invoceBillnoTable);
            //this.GetInvoceBillNo_R(base.Context, this.invoceBillno_RTable);
            this.InsertFinalDetailData(this.tmpRptTbl);
            this.SetRptBosTable(tableName);
        }
        public override List<Kingdee.BOS.Core.Report.SummaryField> GetSummaryColumnInfo(IRptParams filter)
        {
            return new List<Kingdee.BOS.Core.Report.SummaryField>
            {
                new Kingdee.BOS.Core.Report.SummaryField("FORDERAMOUNT", BOSEnums.Enu_SummaryType.SUM),
             //   new Kingdee.BOS.Core.Report.SummaryField("FDELIAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FOUTAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                //new Kingdee.BOS.Core.Report.SummaryField("FRETURNPAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                //new Kingdee.BOS.Core.Report.SummaryField("FRETURNAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                //new Kingdee.BOS.Core.Report.SummaryField("FINVOECEAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                //new Kingdee.BOS.Core.Report.SummaryField("FRECEIPTAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                //new Kingdee.BOS.Core.Report.SummaryField("FWRITEOFFAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FORDERQTY", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FORDERCHARGEQTY", BOSEnums.Enu_SummaryType.SUM),
                //new Kingdee.BOS.Core.Report.SummaryField("FDELIQTY", BOSEnums.Enu_SummaryType.SUM),
                //new Kingdee.BOS.Core.Report.SummaryField("FDELICHARGEQTY", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FOUTQTY", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FOUTCHARGEQTY", BOSEnums.Enu_SummaryType.SUM),
                //new Kingdee.BOS.Core.Report.SummaryField("FRETURNPQTY", BOSEnums.Enu_SummaryType.SUM),
                //new Kingdee.BOS.Core.Report.SummaryField("FRETURNPCHARGEQTY", BOSEnums.Enu_SummaryType.SUM),
                //new Kingdee.BOS.Core.Report.SummaryField("FRETURNQTY", BOSEnums.Enu_SummaryType.SUM),
                //new Kingdee.BOS.Core.Report.SummaryField("FRETURNCHARGEQTY", BOSEnums.Enu_SummaryType.SUM),
                //new Kingdee.BOS.Core.Report.SummaryField("FINVOECEQTY", BOSEnums.Enu_SummaryType.SUM),
                //new Kingdee.BOS.Core.Report.SummaryField("FBILLCHARGEQTY", BOSEnums.Enu_SummaryType.SUM),
                //new Kingdee.BOS.Core.Report.SummaryField("FRECQTY", BOSEnums.Enu_SummaryType.SUM),
                //new Kingdee.BOS.Core.Report.SummaryField("FRECEIVECHARGEQTY", BOSEnums.Enu_SummaryType.SUM),
                //new Kingdee.BOS.Core.Report.SummaryField("FRECAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                //new Kingdee.BOS.Core.Report.SummaryField("FCHARGEOFFAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                //new Kingdee.BOS.Core.Report.SummaryField("FOrderBaseQty", BOSEnums.Enu_SummaryType.SUM),
                //new Kingdee.BOS.Core.Report.SummaryField("FDeliveryNoticeQty", BOSEnums.Enu_SummaryType.SUM),
                //new Kingdee.BOS.Core.Report.SummaryField("FDeliveryQty", BOSEnums.Enu_SummaryType.SUM),
                //new Kingdee.BOS.Core.Report.SummaryField("FReturnNoticeQty", BOSEnums.Enu_SummaryType.SUM),
                //new Kingdee.BOS.Core.Report.SummaryField("FReturnBaseQty", BOSEnums.Enu_SummaryType.SUM),
                //new Kingdee.BOS.Core.Report.SummaryField("FReceiveQty", BOSEnums.Enu_SummaryType.SUM),
                //new Kingdee.BOS.Core.Report.SummaryField("FBaseInvoiceQty", BOSEnums.Enu_SummaryType.SUM)
            };
        }
        public override ReportTitles GetReportTitles(IRptParams filter)
        {
            DynamicObject customFilter = filter.FilterParameter.CustomFilter;
            if (customFilter["SaleOrgList"] == null || (string.IsNullOrWhiteSpace(customFilter["SaleOrgList"].ToString()) && filter.CustomParams.Count <= 1))
            {
                return base.GetReportTitles(filter);
            }
            return this.BuilderTitle(filter);
        }
        private void ModifyShowHideColumns(ref IRptParams filter)
        {
            List<ColumnField> list = new List<ColumnField>(filter.FilterParameter.ColumnInfo);
            List<Kingdee.BOS.Core.CommonFilter.SummaryField> summaryRows = filter.FilterParameter.SummaryRows;
            List<string> list2 = new List<string>();
            foreach (ColumnField current in list)
            {
                list2.Add(current.Key.ToUpperInvariant());
            }
            foreach (Kingdee.BOS.Core.CommonFilter.SummaryField current2 in summaryRows)
            {
                string text = current2.Key.ToUpperInvariant();
                if (!list2.Contains(text))
                {
                    ColumnField columnField = new ColumnField();
                    columnField.Key = text;
                    columnField.FieldName = text;
                    columnField.Visible = true;
                    filter.FilterParameter.ColumnInfo.Add(columnField);
                }
            }
            List<string> list3 = this.SetPriceAmountColumns();
            if (!this.IsCanViewAmount(filter))
            {
                foreach (ColumnField current3 in list)
                {
                    foreach (string current4 in list3)
                    {
                        if (current3.Key.EqualsIgnoreCase(current4))
                        {
                            filter.FilterParameter.ColumnInfo.Remove(current3);
                            break;
                        }
                    }
                }
            }
        }
        private List<string> SetPriceAmountColumns()
        {
            return new List<string>
            {
                "FPRICE",
                "FCharge",
                "FORDERAMOUNT",
                "FDELIAMOUNT",
                "FOUTAMOUNT",
                "FRETURNPAMOUNT",
                "FRETURNAMOUNT",
                "FRECAMOUNT",
                "FWRITEOFFAMOUNT",
                "FINVOECEAMOUNT",
                "FRECEIPTAMOUNT",
                "FCHARGEOFFAMOUNT",
                "FADVANCEAMOUNT"
            };
        }
        public string GetFileter(IRptParams filter, string filterWhere)
        {
            DynamicObject customFilter = filter.FilterParameter.CustomFilter;
            StringBuilder stringBuilder = new StringBuilder();
            string text = string.Empty;
            string text2 = string.Empty;
            string result = string.Empty;
            if (filter.CustomParams.Count<KeyValuePair<string, object>>() > 1)
            {
                Dictionary<string, object> dictionary = (Dictionary<string, object>)filter.CustomParams["ParentReportCurrentRow"];
                result = string.Format(" WHERE TCU.FNUMBER IN ('{0}')", dictionary["FCUSTOMERNUMBER"].ToString());
            }
            else
            {
                if (customFilter["SaleOrgList"] == null || string.IsNullOrWhiteSpace(customFilter["SaleOrgList"].ToString()))
                {
                    stringBuilder.AppendLine("  INNER JOIN T_SAL_ORDER   TSO  ON TCU.FCUSTID=TSO.FCUSTID INNER JOIN  T_SAL_ORDERENTRY  TSOE");
                    stringBuilder.AppendLine(" ON  TSOE.FID=TSO.FID ");
                    stringBuilder.AppendLine(filterWhere);
                    result = stringBuilder.ToString();
                }
                else
                {
                    text = ((this.GetDataByKey(customFilter, "CustomerFrom") == string.Empty) ? "" : this.GetDataByKey(customFilter, "CustomerFrom"));
                    text2 = ((this.GetDataByKey(customFilter, "CustomerTo") == string.Empty) ? "" : this.GetDataByKey(customFilter, "CustomerTo"));
                    if (!string.IsNullOrWhiteSpace(text) && !string.IsNullOrWhiteSpace(text2))
                    {
                        result = string.Format(" WHERE FNUMBER>'{0}' AND FNUMBER<'{1}' ", text, text2);
                    }
                    else
                    {
                        if (!string.IsNullOrWhiteSpace(text) && string.IsNullOrWhiteSpace(text2))
                        {
                            result = string.Format(" WHERE FNUMBER>'{0}'  ", text);
                        }
                        else
                        {
                            if (string.IsNullOrWhiteSpace(text) && !string.IsNullOrWhiteSpace(text2))
                            {
                                result = string.Format(" WHERE  FNUMBER<'{0}' ", text2);
                            }
                            else
                            {
                                result = "";
                            }
                        }
                    }
                }
            }
            return result;
        }
        private ReportTitles BuilderTitle(IRptParams filter)
        {
            ReportTitles reportTitles = new ReportTitles();
            DynamicObject customFilter = filter.FilterParameter.CustomFilter;
            string arg = string.Empty;
            if (customFilter != null && (filter.CustomParams.Count <= 1 || !filter.CustomParams.ContainsKey("ParentReportCurrentRow")))
            {
                if (this.filterOrgList.Trim().Length > 0)
                {
                    reportTitles.AddTitle("FSaleOrgTitle", this.OrgNameList(this.filterOrgList));
                }
                else
                {
                    reportTitles.AddTitle("FSaleOrgTitle", "");
                }
                string arg2 = string.Empty;
                string arg3 = string.Empty;
                DynamicObject dynamicObject = customFilter["SalesFrom"] as DynamicObject;
                string arg4 = string.Empty;
                string arg5 = string.Empty;
                if (dynamicObject != null)
                {
                    arg4 = this.GetDataByKey(dynamicObject, "NUMBER");
                    arg5 = this.GetDataByKey(dynamicObject, "NAME");
                }
                DynamicObject dynamicObject2 = customFilter["SalesTo"] as DynamicObject;
                string arg6 = string.Empty;
                string arg7 = string.Empty;
                if (dynamicObject2 != null)
                {
                    arg6 = this.GetDataByKey(dynamicObject2, "NUMBER");
                    arg7 = this.GetDataByKey(dynamicObject2, "NAME");
                }
                if (dynamicObject != null || dynamicObject2 != null)
                {
                    arg = "--";
                }
                else
                {
                    arg = ResManager.LoadKDString("全部", "004102030003169", SubSystemType.SCM, new object[0]);
                }
                if (dynamicObject != null)
                {
                    arg2 = string.Format("{0}({1})", arg4, arg5);
                }
                if (dynamicObject2 != null)
                {
                    arg3 = string.Format("{0}({1})", arg6, arg7);
                }
                reportTitles.AddTitle("FSalesLimt", string.Format("{0}{1}{2}", arg2, arg, arg3));
                arg = string.Empty;
                string arg8 = string.Empty;
                string arg9 = string.Empty;
                DynamicObject dynamicObject3 = customFilter["CustomerFrom"] as DynamicObject;
                string arg10 = string.Empty;
                string arg11 = string.Empty;
                if (dynamicObject3 != null)
                {
                    arg10 = this.GetDataByKey(dynamicObject3, "NUMBER");
                    arg11 = this.GetDataByKey(dynamicObject3, "NAME");
                }
                DynamicObject dynamicObject4 = customFilter["CustomerTo"] as DynamicObject;
                string arg12 = string.Empty;
                string arg13 = string.Empty;
                if (dynamicObject4 != null)
                {
                    arg12 = this.GetDataByKey(dynamicObject4, "NUMBER");
                    arg13 = this.GetDataByKey(dynamicObject4, "NAME");
                }
                if (dynamicObject3 != null || dynamicObject4 != null)
                {
                    arg = "--";
                }
                else
                {
                    arg = ResManager.LoadKDString("全部", "004102030003169", SubSystemType.SCM, new object[0]);
                }
                if (dynamicObject3 != null)
                {
                    arg8 = string.Format("{0}({1})", arg10, arg11);
                }
                if (dynamicObject4 != null)
                {
                    arg9 = string.Format("{0}({1})", arg12, arg13);
                }
                reportTitles.AddTitle("FCustomerLimit", string.Format("{0}{1}{2}", arg8, arg, arg9));
                arg = string.Empty;
                if (this.filterSFrom != string.Empty || this.filterSTo != string.Empty)
                {
                    arg = "--";
                }
                else
                {
                    if (this.filterSFrom == string.Empty && this.filterSTo == string.Empty)
                    {
                        arg = ResManager.LoadKDString("全部", "004102030003169", SubSystemType.SCM, new object[0]);
                    }
                }
                reportTitles.AddTitle("FIndentLimit", string.Format("{0}{1}{2}", this.filterSFrom, arg, this.filterSTo));
                arg = string.Empty;
                string arg14 = string.Empty;
                string arg15 = string.Empty;
                DynamicObject dynamicObject5 = customFilter["MaterialFrom"] as DynamicObject;
                string text = string.Empty;
                string arg16 = string.Empty;
                if (dynamicObject5 != null)
                {
                    text = this.GetDataByKey(dynamicObject5, "NUMBER");
                    arg16 = this.GetDataByKey(dynamicObject5, "NAME");
                }
                DynamicObject dynamicObject6 = customFilter["MaterialTo"] as DynamicObject;
                string text2 = string.Empty;
                string arg17 = string.Empty;
                if (dynamicObject6 != null)
                {
                    text2 = this.GetDataByKey(dynamicObject6, "NUMBER");
                    arg17 = this.GetDataByKey(dynamicObject6, "NAME");
                }
                if (dynamicObject5 != null || dynamicObject6 != null)
                {
                    arg = "--";
                }
                else
                {
                    arg = ResManager.LoadKDString("全部", "004102030003169", SubSystemType.SCM, new object[0]);
                }
                if (text != string.Empty)
                {
                    arg14 = string.Format("{0}({1})", text, arg16);
                }
                if (text2 != string.Empty)
                {
                    arg15 = string.Format("{0}({1})", text2, arg17);
                }
                reportTitles.AddTitle("FMaterialLimit", string.Format("{0}{1}{2}", arg14, arg, arg15));
                arg = string.Empty;
            }
            else
            {
                Dictionary<string, object> dictionary = (Dictionary<string, object>)filter.CustomParams["ParentReportCurrentRow"];
                reportTitles.AddTitle("FSaleOrgTitle", Convert.ToString(dictionary["FSALEORGNAME"]));
                if (dictionary["FSALERNUMBER"].ToString().Trim() != string.Empty && dictionary["FSALERNUMBER"].ToString().Trim() != "0")
                {
                    reportTitles.AddTitle("FSalesLimt", string.Format("{0}({1})", dictionary["FSALERNUMBER"].ToString(), dictionary["FSALES"].ToString()));
                }
                else
                {
                    reportTitles.AddTitle("FSalesLimt", "");
                }
                if (dictionary["FCUSTOMERNUMBER"].ToString().Trim() != string.Empty && dictionary["FCUSTOMERNUMBER"].ToString().Trim() != "0")
                {
                    reportTitles.AddTitle("FCustomerLimit", string.Format("{0}({1})", dictionary["FCUSTOMERNUMBER"].ToString(), Convert.ToString(dictionary["FCUSTOMERNAME"])));
                }
                else
                {
                    reportTitles.AddTitle("FCustomerLimit", "");
                }
                reportTitles.AddTitle("FMaterialLimit", string.Format("{0}({1})", dictionary["FMATERIALNUMBER"].ToString(), Convert.ToString(dictionary["FMATERIALNAME"])));
                reportTitles.AddTitle("FIndentLimit", dictionary["FBILLNO"].ToString());
            }
            string text3 = (this.filterSoDayFrom == DateTime.MinValue) ? "" : FieldFormatterUtil.GetDateFormatString(base.Context, this.filterSoDayFrom);
            string text4 = (this.filterSoDayTo == DateTime.MaxValue) ? "" : FieldFormatterUtil.GetDateFormatString(base.Context, this.filterSoDayTo);
            if (!text3.IsNullOrEmptyOrWhiteSpace() || !text4.IsNullOrEmptyOrWhiteSpace())
            {
                arg = ResManager.LoadKDString(" 至 ", "004102030003172", SubSystemType.SCM, new object[0]);
            }
            reportTitles.AddTitle("FDeliveryStartDay", string.Format("{0}{1}{2}", text3, arg, text4));
            arg = string.Empty;
            return reportTitles;
        }
        public void GetInvoceBillNo_R(Context ctx, string invoceBillno_RTable)
        {
            new List<SqlObject>();
            string strSQL = string.Format("select TIVS.FSRCROWID,TIV.FBILLNO \r\n                                        FROM T_IV_SALESICENTRY TIVS INNER JOIN {0} TEMP1 ON TIVS.FSRCROWID=TEMP1.FRECID\r\n                                        LEFT JOIN T_IV_SALESIC TIV ON TIV.FID=TIVS.FID AND TIV.FDOCUMENTSTATUS='C'\r\n                                        Order By TIVS.FSRCROWID ASC ", this.flowTable);
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
                num = Convert.ToInt64(dynamicObject["FSRCROWID"]);
                text = Convert.ToString(dynamicObject["FBILLNO"]);
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
                    if (num == Convert.ToInt64(dynamicObject["FSRCROWID"]))
                    {
                        text = Convert.ToString(dynamicObject["FBILLNO"]);
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
                        num = Convert.ToInt64(dynamicObject["FSRCROWID"]);
                        list.Clear();
                        text = Convert.ToString(dynamicObject["FBILLNO"]);
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
            num = Convert.ToInt64(dynamicObject["FSRCROWID"]);
            list.Clear();
            if (dictionary != null)
            {
                stringBuilder.Clear();
                DataTable dataTable = new DataTable(invoceBillno_RTable);
                dataTable.Columns.Add("FSRCROWID", typeof(long));
                dataTable.Columns.Add("FINVOCEBILLNO_R", typeof(string));
                foreach (KeyValuePair<long, string> current in dictionary)
                {
                    DataRow dataRow = dataTable.NewRow();
                    dataRow["FSRCROWID"] = current.Key;
                    dataRow["FINVOCEBILLNO_R"] = current.Value;
                    dataTable.Rows.Add(dataRow);
                }
                DBUtils.BulkInserts(ctx, dataTable);
            }
        }

        //最终取数
        private void InsertFinalDetailData(string rptTmpTable)
        {
            List<SqlObject> list = new List<SqlObject>();
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine(string.Format(" INSERT /*+append*/ INTO {0} ", rptTmpTable));
            stringBuilder.AppendLine(" (");
            stringBuilder.AppendLine("  FID,FORDERID,FORDERNAME ");
            stringBuilder.AppendLine("  ,FSEQ,FSALEORGID,FSALEORGNAME,FBILLNO,FDATE, FSALEDEPTID,FSALEDEPT");
            stringBuilder.AppendLine("  ,FSALEGROUPID ,FSALEGROUP,FSALERID,FSALES");
            stringBuilder.AppendLine("  ,FCUSTID,FCUSTOMERNUMBER,FCUSTOMERNAME,FDELIVERYDAY");
            stringBuilder.AppendLine("  ,FMATERIALID,FMATERIALNUMBER,FMATERIALNAME,FMATERIALGROUP,FMATERIALGROUPNAME");
            stringBuilder.AppendLine("  ,FMAPID,FMAPNUMBER,FMAPNAME");
            stringBuilder.AppendLine("  ,FSPECIFICATION,FAuxPropId,FAuxProp ");
            stringBuilder.AppendLine("  ,FUNITID ,FUNIT,FPRICEUNITID,FPRICEUNIT,FCURRENCYID,FMONEYTYPE,FPRICE,FCHARGE");
            stringBuilder.AppendLine(" ,FORDERQTY,FORDERCHARGEQTY,FORDERAMOUNT--,FDELIID,FDELIBILLNO,FDELIQTY,FDELIPRICEUNITID,FDELICHARGEQTY,FDELIAMOUNT");
            stringBuilder.AppendLine(" ,FOUTID,FOUTBILLNO,FOUTQTY,FOUTPRICEUNITID,FOUTCHARGEQTY,FOUTAMOUNT");

            stringBuilder.AppendLine(" ,FMOBILLNO,FMODATE,FMOQTY,FMOINSTOCKDATE");
            stringBuilder.AppendLine(" ,FMOINSTOCKQTY,FPURORDERNO,FPURORDERDATE,FPURORDERQTY");

            stringBuilder.AppendLine(" ,FPURINSTOCKDATE,FPURINSTOCKQTY");

            stringBuilder.AppendLine(" ,FOPENO,FOPEDATE,FOPEQTY");

            stringBuilder.AppendLine(" ,FPLANNO,FPLANDATE,FPLANQTY");



            //stringBuilder.AppendLine("  ,FRETURNPRDID,FRETURNPRDBILLNO,FRETURNPQTY,FRETURNPPRICEUNITID,FRETURNPCHARGEQTY,FRETURNPAMOUNT");
            //stringBuilder.AppendLine("  ,FRETURNID,FRETURNBILLNO,FRETURNQTY,FRETURNPRICEUNITID,FRETURNCHARGEQTY,FRETURNAMOUNT");
            //stringBuilder.AppendLine("  ,FRECEIVEBILLNO,FRECQTY,FRECEPRICEUNITID,FRECEIVECHARGEQTY,FRECAMOUNT,FWRITEOFFAMOUNT,FSETACCOUNTTYPE");
            //stringBuilder.AppendLine(" ,FINVOECEBILLNO");
            //stringBuilder.AppendLine(" ,FINVOECEQTY,FBILLCHARGEQTY,FINVOECEAMOUNT");
            //stringBuilder.AppendLine(" ,FRECEIPTAMOUNT,FNRECEIPTAMOUNT,FRECSUMAMOUNNT,FCHARGEOFFAMOUNT,FRECBILLID ");
            //stringBuilder.AppendLine(" ,FRECBILLNO,FCHARGEOFFBILLNO");
            stringBuilder.AppendLine(" ,FPRECISION,FCHARGEPRECISION,FPRICEDIGITS,FAMOUNTDIGITS");
            stringBuilder.AppendLine(" ,FCLOSESTATUS,FMRPCLOSESTATUS,FMRPTERMINATESTATUS");
            stringBuilder.AppendLine(" ,FBASEUNITID_ID,FBASEUNITID,FORDERBASEQTY,FBASEPRICE--,FDELIVERYNOTICEQTY,FDELIVERYQTY,FRETURNNOTICEQTY,FRETURNBASEQTY,FRECEIVEQTY,FBASEINVOICEQTY");
            stringBuilder.AppendLine(" --,FISRECMAT");
            stringBuilder.AppendLine(" )");
            stringBuilder.AppendLine(" ");
            stringBuilder.AppendLine(" ");
            stringBuilder.AppendLine(" SELECT  TSO.FID,TSOE.FENTRYID");
            stringBuilder.AppendLine(string.Format(" ,TSOE.FSEQ as FORDERNAME ", ""));
            stringBuilder.AppendLine("  ,T1.FSEQ");
            stringBuilder.AppendLine("  ,TSO.FSALEORGID,TORGL.FNAME AS FORGNAME,TSO.FBILLNO AS FBILLNO,TSO.FDATE");
            stringBuilder.AppendLine(" ,TSO.FSALEDEPTID,TBD.FNAME AS FDEPTNAME,TSO.FSALEGROUPID, VBO.FNAME AS FSALEGROUP");
            stringBuilder.AppendLine(" ,TSO.FSALERID,VBS.FNAME AS SALESNAME");
            stringBuilder.AppendLine(" ,TBDC.FCUSTID, TBDC.FNUMBER AS FCUSTOMERNUMBER,TBC.FNAME AS FCUSTNAME,TSRD.FDELIVERYDATE AS FDELIVERYDATE");
            stringBuilder.AppendLine(" ,TSOE.FMATERIALID,TBDM.FNUMBER AS FMATERIALNUMBER,TBM.FNAME AS FMATERIALNAME");
            stringBuilder.AppendLine("  ,TBDM.FMATERIALGROUP,TGOU.FNAME FMATERIALGROUPNAME");
            stringBuilder.AppendLine("  ,TSOE.FMAPID,VCM.FNUMBER AS FMAPNUMBER,VCML.FNAME AS FMAPNAME");
            stringBuilder.AppendLine("  ,TBM.FSPECIFICATION,TSOE.FAuxPropId,'' as FAuxProp ");
            stringBuilder.AppendLine(" ,TSOE.FUNITID,TBU.FNAME AS FUNITNAME,TSOF.FPRICEUNITID,TBU2.FNAME AS FPRICEUNIT,TBCL.FCURRENCYID, TBCL.FNAME AS FMONEYTYPE ");
            stringBuilder.AppendLine(" ,(CASE WHEN (TSOE.FBaseUnitQty=0 or T1.FBASEORDERQTY*TSOE.FQty/TSOE.FBaseUnitQty=0) THEN TSOF.FALLAMOUNT  ELSE  TSOF.FALLAMOUNT/(T1.FBASEORDERQTY*TSOE.FQty/TSOE.FBaseUnitQty) END) AS FPRICE ");
            if (this.priceFrom.EqualsIgnoreCase("RECEIVEBILL"))
            {
                stringBuilder.AppendLine(" ,(CASE WHEN TARS.FPRICEQTY=0 THEN TARS.FALLAMOUNT ELSE TARS.FALLAMOUNT/TARS.FPRICEQTY END) AS FCHARGE ");
            }
            else
            {
                stringBuilder.AppendLine(" ,(CASE WHEN ISNULL(T1.FBASEORDERQTY,0)=0  THEN null ELSE TSOF.FALLAMOUNT/(CASE WHEN ISNULL(TSOF.FPriceUnitQty,0) = 0 THEN NULL ELSE TSOF.FPriceUnitQty END) END) AS FCHARGE ");
            }
            stringBuilder.AppendLine(" ,(CASE WHEN TSOE.FBaseUnitQty=0 THEN T1.FBASEORDERQTY ELSE T1.FBASEORDERQTY*TSOE.FQty/TSOE.FBaseUnitQty END) AS FORDERQTY");
            stringBuilder.AppendLine(" ,TSOF.FPriceUnitQty AS FORDERCHARGEQTY");
            stringBuilder.AppendLine("  ,(CASE WHEN ISNULL(T1.FBASEORDERQTY,0)=0  THEN null ELSE  TSOF.FALLAMOUNT END) AS FORDERAMOUNT ");
            //stringBuilder.AppendLine(" ,CASE WHEN TSD.FDOCUMENTSTATUS='C' THEN  TSDE.FEntryID  ELSE 0 END AS FDELIID ");
            //stringBuilder.AppendLine(" ,CASE WHEN TSD.FDOCUMENTSTATUS='C' THEN  TSD.FBILLNO  ELSE NULL END AS FDELIBILLNO ");
            //stringBuilder.AppendLine(" ,TSDE.FQTY AS FDELIQTY");
            //stringBuilder.AppendLine(" ,TSDEF.FPRICEUNITID AS FDELIPRICEUNITID");
            //stringBuilder.AppendLine(" ,TSDEF.FPriceUnitQty AS FDELICHARGEQTY");
            //stringBuilder.AppendLine(" ,(CASE WHEN ISNULL(T1.FBASECAlCQTY,0)=0  THEN NULL ELSE  T1.FBASEDENOQTY*TSOF.FALLAMOUNT/T1.FBASECAlCQTY END) AS FDELIAMOUNT");
            stringBuilder.AppendLine("  , CASE WHEN TSOSE.FENTRYID IS NOT NULL AND (TSOS.FDOCUMENTSTATUS='C' OR TSOSE.FSTOCKFLAG=1 ) AND T1.FOUTTYPE = 1 THEN TSOSE.FEntryId\r\n                                        WHEN TINITOUTE.FENTRYID IS NOT NULL AND TINIT.FBILLTYPEID ='5518f5ceee8053' AND T1.FOUTTYPE = 2 THEN TINITOUTE.FENTRYID\r\n                                   ELSE 0 END AS FOUTID ");
            stringBuilder.AppendLine("  , CASE WHEN  TSOSE.FENTRYID IS NOT NULL AND ( TSOS.FDOCUMENTSTATUS='C' OR TSOSE.FSTOCKFLAG=1 )  AND T1.FOUTTYPE = 1 THEN TSOS.FBILLNO \r\n                                        WHEN  TINITOUTE.FENTRYID IS NOT NULL AND TINIT.FBILLTYPEID ='5518f5ceee8053' AND T1.FOUTTYPE = 2 THEN  TINIT.FBILLNO\r\n                                    ELSE NULL END AS FOUTBILLNO ");
            stringBuilder.AppendLine("  ,TSOSEF.FSALUNITQTY AS FOUTQTY");
            stringBuilder.AppendLine("  ,TSOSEF.FPRICEUNITID AS FOUTPRICEUNITID");
            stringBuilder.AppendLine("  ,TSOSEF.FPRICEUNITQTY AS FOUTCHARGEQTY");
            stringBuilder.AppendLine(" ,(CASE WHEN ISNULL(T1.FBASECAlCQTY,0)=0  THEN NULL ELSE  T1.FBASEOUTQTY*TSOF.FALLAMOUNT/T1.FBASECAlCQTY END) AS FOUTAMOUNT");

            stringBuilder.AppendLine(" ,Mo.FBILLNO ,MO.FDATE ");
            stringBuilder.AppendLine(" ,(CASE WHEN ISNULL(T1.FBASECAlCQTY,0)=0  THEN NULL ELSE  T1.FBASEMOQTY*TSOE.FQty/T1.FBASECAlCQTY END) AS FMOQTY");
            stringBuilder.AppendLine(" ,PRDIN.FDATE ");
            stringBuilder.AppendLine(" ,(CASE WHEN ISNULL(T1.FBASECAlCQTY,0)=0  THEN NULL ELSE  T1.FBASEPRDINQTY*TSOE.FQty/T1.FBASECAlCQTY END) AS FPRDINQTY");

            stringBuilder.AppendLine(" ,PURO.FBILLNO ,PURO.FDATE ");
            stringBuilder.AppendLine(" ,(CASE WHEN ISNULL(T1.FBASECAlCQTY,0)=0  THEN NULL ELSE  T1.FBASEPURORDERQTY*TSOE.FQty/T1.FBASECAlCQTY END) AS FPURORDERQTY");
            stringBuilder.AppendLine(" ,STK.FDATE ");
            stringBuilder.AppendLine(" ,(CASE WHEN ISNULL(T1.FBASECAlCQTY,0)=0  THEN NULL ELSE  T1.FBASEPURINSTOCKQTY*TSOE.FQty/T1.FBASECAlCQTY END) AS FPURINSTOCKQTY");

            stringBuilder.AppendLine(" ,TOPE.FBILLNO,TOPE.FPLANSTARTTIME");
            stringBuilder.AppendLine(" ,(CASE WHEN ISNULL(T1.FBASECAlCQTY,0)=0  THEN NULL ELSE  TOPE.FBaseQty*TSOE.FQty/T1.FBASECAlCQTY END) AS FOPQTY");

            stringBuilder.AppendLine(" ,TPLN.FBILLNO,TPLN.FDEMANDDATE");
            stringBuilder.AppendLine(" ,(CASE WHEN ISNULL(T1.FBASECAlCQTY,0)=0  THEN NULL ELSE  TPLN.FBASEORDERQTY*TSOE.FQty/T1.FBASECAlCQTY END) AS FPLNQTY");





            stringBuilder.AppendLine(" ,TBUT.FPRECISION ");
            stringBuilder.AppendLine(" ,TBUT2.FPRECISION AS FCHARGEPRECISION");
            stringBuilder.AppendLine(" ,TBCU.FPRICEDIGITS");
            stringBuilder.AppendLine(" ,TBCU.FAMOUNTDIGITS");
            stringBuilder.AppendLine(" ,CASE WHEN TSO.FCLOSESTATUS='B' THEN  'Y'  ELSE ' ' END");
            stringBuilder.AppendLine(" ,CASE WHEN TSOE.FMRPCLOSESTATUS='B' THEN  'Y'  ELSE ' ' END");
            stringBuilder.AppendLine(" ,CASE WHEN TSOE.FMRPTERMINATESTATUS='B' THEN  'Y'  ELSE 'N' END");
            stringBuilder.AppendLine(" ,TSOE.FBASEUNITID,TBU3.FNAME AS FBASEUNITNAME");
            stringBuilder.AppendLine(" ,TSOE.FBASEUNITQTY");
            stringBuilder.AppendLine(" ,(CASE WHEN ISNULL(TSOE.FBASEUNITQTY,0)=0 THEN 0  ELSE  ISNULL(TSOF.FAMOUNT,0)/TSOE.FBASEUNITQTY END) AS FBASEPRICE ");

            stringBuilder.AppendLine("");
            stringBuilder.AppendLine(string.Format(" FROM {0} T1 ", this.flowTable));
            stringBuilder.AppendLine(" LEFT  JOIN T_SAL_ORDERENTRY  TSOE  ");
            stringBuilder.AppendLine(" ON T1.FORDERID=TSOE.FENTRYID ");
            stringBuilder.AppendLine(" LEFT  JOIN T_SAL_ORDER TSO  ");
            stringBuilder.AppendLine(" ON TSOE.FID=TSO.FID");
            stringBuilder.AppendLine(" INNER  JOIN T_SAL_ORDERENTRY_F TSOF ");
            stringBuilder.AppendLine(" ON TSOE.FENTRYID=TSOF.FENTRYID ");
            stringBuilder.AppendLine(" INNER JOIN T_SAL_ORDERENTRY_D TSRD  ");
            stringBuilder.AppendLine(" ON T1.FORDERID=TSRD.FENTRYID ");
            stringBuilder.AppendLine(" LEFT  JOIN T_PRD_MOENTRY MOE ");
            stringBuilder.AppendLine(" ON T1.FMOID=MOE.FENTRYID");
            stringBuilder.AppendLine(" LEFT  JOIN T_PRD_MO MO ");
            stringBuilder.AppendLine(" ON MOE.FID=MO.FID");
            stringBuilder.AppendLine(" LEFT  JOIN T_PRD_INSTOCKENTRY PRDINE ");
            stringBuilder.AppendLine(" ON T1.FPRDINSTOCKID=PRDINE.FENTRYID");
            stringBuilder.AppendLine(" LEFT  JOIN T_PRD_INSTOCK PRDIN ");
            stringBuilder.AppendLine(" ON PRDINE.FID=PRDIN.FID");

            stringBuilder.AppendLine(" LEFT  JOIN T_PUR_POORDERENTRY PUROE ");
            stringBuilder.AppendLine(" ON T1.FPURORDERID=PUROE.FENTRYID");
            stringBuilder.AppendLine(" LEFT  JOIN T_PUR_POORDER PURO ");
            stringBuilder.AppendLine(" ON PUROE.FID=PURO.FID");
            stringBuilder.AppendLine(" LEFT  JOIN T_STK_INSTOCKENTRY STKE ");
            stringBuilder.AppendLine(" ON T1.FPURINSTOCKID=STKE.FENTRYID");
            stringBuilder.AppendLine(" LEFT  JOIN T_STK_INSTOCK STK ");
            stringBuilder.AppendLine(" ON STKE.FID=STK.FID");

            stringBuilder.AppendLine(" LEFT JOIN T_SFC_OPERPLANNING TOPE on TOPE.FMOENTRYID=T1.FMOID");

            stringBuilder.AppendLine(" LEFT JOIN T_PLN_PLANORDER_B TPLNB on TPLNB.FSaleOrderEntryId=T1.FORDERID");
            //stringBuilder.AppendLine(" LEFT JOIN  T_PLN_PLANORDER TPLN ON TPLN.FID = TPLNB.FID ");
            //stringBuilder.AppendLine(string.Format(" LEFT JOIN T_ENG_ROUTE_L TOUL on TOUL.FMOENTRYID=T1.FMOID and TOL.FLOCALEID={0} ", base.Context.UserLocale.LCID.ToString()));

            stringBuilder.AppendLine(" LEFT  JOIN T_SAL_OUTSTOCKENTRY TSOSE ");
            stringBuilder.AppendLine(" ON T1.FOUTID=TSOSE.FENTRYID AND T1.FOUTTYPE ='1'");
            stringBuilder.AppendLine(" LEFT  JOIN T_SAL_OUTSTOCK TSOS ");
            stringBuilder.AppendLine(" ON TSOSE.FID=TSOS.FID ");
            stringBuilder.AppendLine(" LEFT JOIN T_SAL_OUTSTOCKENTRY_F TSOSEF ");
            stringBuilder.AppendLine(" ON TSOSE.FENTRYID = TSOSEF.FENTRYID ");
            stringBuilder.AppendLine(" LEFT  JOIN T_SAL_INITOUTSTOCKENTRY TINITOUTE ");
            stringBuilder.AppendLine(" ON ((T1.FOUTID=TINITOUTE.FENTRYID ) AND TSO.FISINIT='1' AND T1.FOUTTYPE ='2')");
            stringBuilder.AppendLine(" LEFT  JOIN T_SAL_INITOUTSTOCK TINIT ");
            stringBuilder.AppendLine(" ON TINITOUTE.FID=TINIT.FID ");

            stringBuilder.AppendLine(" LEFT  JOIN T_BD_DEPARTMENT_L TBD ");
            stringBuilder.AppendLine(string.Format(" ON TSO.FSALEDEPTID=TBD.FDEPTID AND TBD.FLOCALEID={0} ", base.Context.UserLocale.LCID.ToString()));
            stringBuilder.AppendLine(" LEFT  JOIN T_BD_DEPARTMENT TBDE ");
            stringBuilder.AppendLine(" ON TSO.FSALEDEPTID=TBDE.FDEPTID ");
            stringBuilder.AppendLine(" LEFT  JOIN T_BD_CUSTOMER_L TBC  ");
            stringBuilder.AppendLine(string.Format(" ON TSO.FCUSTID=TBC.FCUSTID  AND TBC.FLOCALEID={0} ", base.Context.UserLocale.LCID.ToString()));
            stringBuilder.AppendLine(" LEFT  JOIN V_BD_SALESMAN_L VBS ");
            stringBuilder.AppendLine(string.Format(" ON TSO.FSALERID=VBS.FID AND VBS.FLOCALEID={0} ", base.Context.UserLocale.LCID.ToString()));
            stringBuilder.AppendLine(" LEFT  JOIN V_BD_SALESMAN VBSE  ");
            stringBuilder.AppendLine(" ON TSO.FSALERID=VBSE.FID ");
            stringBuilder.AppendLine(" LEFT  JOIN T_BD_MATERIAL_L TBM ");
            stringBuilder.AppendLine(string.Format(" ON TBM.FMATERIALID=TSOE.FMATERIALID AND TBM.FLOCALEID={0} ", base.Context.UserLocale.LCID.ToString()));
            stringBuilder.AppendLine(" LEFT  JOIN V_BD_OPERATORGROUP_L VBO  ");
            stringBuilder.AppendLine(string.Format(" ON TSO.FSALEGROUPID=VBO.FENTRYID AND VBO.FLOCALEID={0} ", base.Context.UserLocale.LCID.ToString()));
            stringBuilder.AppendLine(" LEFT  JOIN T_BD_MATERIAL TBDM  ");
            stringBuilder.AppendLine(" ON TSOE.FMATERIALID=TBDM.FMATERIALID ");
            stringBuilder.AppendLine(" LEFT  JOIN V_SAL_CUSTMATMAPPING VCM  ");
            stringBuilder.AppendLine(" ON TSOE.FMAPID=VCM.FID ");
            stringBuilder.AppendLine(" LEFT  JOIN V_SAL_CUSTMATMAPPING_L VCML  ");
            stringBuilder.AppendLine(" ON VCM.FID=VCML.FID ");
            stringBuilder.AppendLine(string.Format("  LEFT JOIN T_BD_MATERIALGROUP_L TGOU ON TGOU.FID=TBDM.FMATERIALGROUP AND TGOU.FLOCALEID={0} ", base.Context.UserLocale.LCID));
            stringBuilder.AppendLine(" LEFT  JOIN t_BD_Unit_L TBU  ");
            stringBuilder.AppendLine(string.Format(" ON TBU.FUNITID=TSOE.FUNITID AND  TBU.FLOCALEID={0} ", base.Context.UserLocale.LCID.ToString()));
            stringBuilder.AppendLine(" LEFT  JOIN t_BD_Unit_L TBU2  ");
            stringBuilder.AppendLine(string.Format(" ON TBU2.FUNITID=TSOF.FPRICEUNITID AND  TBU2.FLOCALEID={0} ", base.Context.UserLocale.LCID.ToString()));
            stringBuilder.AppendLine(" LEFT  JOIN t_BD_Unit_L TBU3  ");
            stringBuilder.AppendLine(string.Format(" ON TBU3.FUNITID=TSOE.FBASEUNITID AND  TBU3.FLOCALEID={0} ", base.Context.UserLocale.LCID.ToString()));
            stringBuilder.AppendLine(" LEFT  JOIN T_BD_CUSTOMER TBDC ");
            stringBuilder.AppendLine(" ON TSO.FCUSTID=TBDC.FCUSTID ");
            stringBuilder.AppendLine(" INNER JOIN T_SAL_ORDERFIN  TSF  ");
            stringBuilder.AppendLine(" ON TSO.FID=TSF.FID");
            stringBuilder.AppendLine(" LEFT  JOIN T_BD_CURRENCY_L TBCL ");
            stringBuilder.AppendLine(string.Format(" ON TSF.FSETTLECURRID=TBCL.FCURRENCYID AND TBCL.FLOCALEID={0}", base.Context.UserLocale.LCID.ToString()));
            stringBuilder.AppendLine(" LEFT JOIN T_BD_RECCONDITION TBR ");
            stringBuilder.AppendLine(string.Format(" ON TSF.FRECCONDITIONID=TBR.FID ", new object[0]));
            stringBuilder.AppendLine(" LEFT  JOIN T_ORG_ORGANIZATIONS  TORG ");
            stringBuilder.AppendLine(" ON TSO.FSALEORGID=TORG.FORGID");
            stringBuilder.AppendLine(" LEFT  JOIN T_ORG_ORGANIZATIONS_L TORGL ");
            stringBuilder.AppendLine(string.Format(" ON TSO.FSALEORGID=TORGL.FORGID AND TORGL.FLOCALEID={0}", base.Context.UserLocale.LCID.ToString()));
            stringBuilder.AppendLine(" LEFT  JOIN T_BD_CURRENCY TBCU ");
            stringBuilder.AppendLine(" ON TSF.FSETTLECURRID=TBCU.FCURRENCYID");
            stringBuilder.AppendLine(" LEFT  JOIN T_BD_UNIT TBUT ");
            stringBuilder.AppendLine(" ON TSOE.FUNITID=TBUT.FUNITID");
            stringBuilder.AppendLine(" LEFT  JOIN T_BD_UNIT TBUT2 ");
            stringBuilder.AppendLine(" ON TSOF.FPRICEUNITID = TBUT2.FUNITID");
  
            list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
            DBUtils.ExecuteBatchWithTime(base.Context, list, 180);
            list.Clear();
            string sql = string.Format(" IF NOT EXISTS (SELECT 1 FROM KSQL_INDEXES WHERE KSQL_INDNAME = 'idx_{0}_1') CREATE INDEX idx_{0}_1 ON {1} (FORDERID)", rptTmpTable.Substring(3, 22), rptTmpTable);
            list.Add(new SqlObject(sql, new List<SqlParam>()));
            //sql = string.Format("  IF NOT EXISTS (SELECT 1 FROM KSQL_INDEXES WHERE KSQL_INDNAME = 'idx_{0}_2') CREATE INDEX idx_{0}_2 ON {1} (FDELIID)", rptTmpTable.Substring(3, 22), rptTmpTable);
            //list.Add(new SqlObject(sql, new List<SqlParam>()));
            sql = string.Format("  IF NOT EXISTS (SELECT 1 FROM KSQL_INDEXES WHERE KSQL_INDNAME = 'idx_{0}_3') CREATE INDEX idx_{0}_3 ON {1} (FOUTID)", rptTmpTable.Substring(3, 22), rptTmpTable);
            list.Add(new SqlObject(sql, new List<SqlParam>()));
            //sql = string.Format("  IF NOT EXISTS (SELECT 1 FROM KSQL_INDEXES WHERE KSQL_INDNAME = 'idx_{0}_4') CREATE INDEX idx_{0}_4 ON {1} (FRETURNID)", rptTmpTable.Substring(3, 22), rptTmpTable);
            //list.Add(new SqlObject(sql, new List<SqlParam>()));
            //sql = string.Format("  IF NOT EXISTS (SELECT 1 FROM KSQL_INDEXES WHERE KSQL_INDNAME = 'idx_{0}_5') CREATE INDEX idx_{0}_5 ON {1} (FRECBILLID)", rptTmpTable.Substring(3, 22), rptTmpTable);
            //list.Add(new SqlObject(sql, new List<SqlParam>()));
            sql = string.Format("  IF NOT EXISTS (SELECT 1 FROM KSQL_INDEXES WHERE KSQL_INDNAME = 'idx_{0}_6') CREATE INDEX idx_{0}_6 ON {1} (FSEQ)", rptTmpTable.Substring(3, 22), rptTmpTable);
            list.Add(new SqlObject(sql, new List<SqlParam>()));
            DBUtils.ExecuteBatch(base.Context, list);
            list.Clear();
            using (KDTransactionScope kDTransactionScope = new KDTransactionScope(TransactionScopeOption.Suppress))
            {
                DbOptimizationUtil dbOptimizationUtil = new DbOptimizationUtil(base.Context, new HashSet<string>
                {
                    rptTmpTable
                });
                DBUtils.ExecuteBatch(base.Context, dbOptimizationUtil.GetStatisticSql(), 1);
                kDTransactionScope.Complete();
            }
            if (!this.isRecWithMat)
            {
                //stringBuilder.AppendLine(string.Format(" INSERT /*+append*/ INTO {0} ", this.recAmountTable));
                //stringBuilder.AppendLine(" (");
                //stringBuilder.AppendLine(" FSEQ,FRECBILLID,FRECEIPTAMOUNT ");
                //stringBuilder.AppendLine(" )");
                //stringBuilder.AppendLine(" SELECT MAX(T1.FSEQ),T1.FRECBILLID,T1.FRECSUMAMOUNNT-SUM(ISNULL(T2.FRECEIPTAMOUNT,0)) FRECEIPTAMOUNT  ");
                //stringBuilder.AppendLine(string.Format(" FROM  ( SELECT T0.FRECBILLID,T0.FRECSUMAMOUNNT,MAX(T0.FSEQ) FSEQ FROM {0} T0 GROUP BY T0.FRECBILLID,T0.FRECSUMAMOUNNT ) T1 ", rptTmpTable));
                //stringBuilder.AppendLine(string.Format("  INNER JOIN  {0} T2 ", rptTmpTable));
                //stringBuilder.AppendLine("   ON T2.FSEQ!=T1.FSEQ AND T1.FRECBILLID=T2.FRECBILLID");
                //stringBuilder.AppendLine("   GROUP BY T1.FRECBILLID ,T1.FRECSUMAMOUNNT ");
                //list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
                //stringBuilder.Clear();
                //stringBuilder.AppendLine(string.Format("/*dialect*/ merge into {0}  P using\r\n                                (SELECT T0.FRECBILLID,T0.FSEQ,T0.FRECEIPTAMOUNT FROM {1} T0) UT\r\n                                on (UT.FRECBILLID=P.FRECBILLID and UT.FSEQ=P.FSEQ)\r\n                                WHEN MATCHED THEN UPDATE SET P.FRECEIPTAMOUNT=UT.FRECEIPTAMOUNT {2}", rptTmpTable, this.recAmountTable, (base.Context.DatabaseType == DatabaseType.MS_SQL_Server) ? ";" : ""));
                //list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
                //stringBuilder.Clear();
            }
            //stringBuilder.AppendLine(string.Format("/*dialect*/ MERGE INTO {0} T0 USING\r\n                               (  SELECT BB.FORDERID,BB.FSEQ,SUM(TSOPE.FAMOUNT) AS FADVANCEAMOUNT \r\n                                  FROM (SELECT FORDERID,MIN(FSEQ) FSEQ FROM {0} TM WHERE TM.FISRECMAT = 1\r\n                                        GROUP BY FORDERID) BB \r\n                                        INNER JOIN T_SAL_ORDERPLAN TSOP ON BB.FORDERID = TSOP.FOrderEntryId AND TSOP.FNEEDRECADVANCE = 1\r\n                                        INNER JOIN T_SAL_ORDERPLANENTRY TSOPE ON TSOPE.FENTRYID = TSOP.FENTRYID GROUP BY BB.FORDERID,BB.FSEQ) UT ON (T0.FORDERID = UT.FORDERID AND T0.FSEQ = UT.FSEQ)\r\n                            WHEN MATCHED THEN UPDATE SET T0.FADVANCEAMOUNT = UT.FADVANCEAMOUNT {1}", rptTmpTable, (base.Context.DatabaseType == DatabaseType.MS_SQL_Server) ? ";" : ""));
            //list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            //stringBuilder.Clear();
            //stringBuilder.AppendLine(string.Format("/*dialect*/ MERGE INTO {0} T0 USING\r\n                               (\r\n                            SELECT NT.FID,SUM(NT.FADVANCEAMOUNT) AS FADVANCEAMOUNT FROM \r\n                                  (SELECT DISTINCT TSO.FID,TSOPE.FENTRYID,TSOPE.FADVANCEENTRYID,TSOPE.FAMOUNT AS FADVANCEAMOUNT FROM {0} TM\r\n                                INNER JOIN T_SAL_ORDER TSO ON TSO.FID = TM.FID AND TM.FISRECMAT = 0\r\n                                INNER JOIN T_SAL_ORDERPLAN TSOP ON TSO.FID = TSOP.FID AND TSOP.FNEEDRECADVANCE = 1\r\n                                INNER JOIN T_SAL_ORDERPLANENTRY TSOPE ON TSOPE.FENTRYID = TSOP.FENTRYID) NT GROUP BY NT.FID\r\n                              ) UT ON (T0.FID = UT.FID)\r\n                            WHEN MATCHED THEN UPDATE SET T0.FADVANCEAMOUNT = UT.FADVANCEAMOUNT {1}", rptTmpTable, (base.Context.DatabaseType == DatabaseType.MS_SQL_Server) ? ";" : ""));
            //list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            //stringBuilder.Clear();
            //if (base.Context.DatabaseType == DatabaseType.MS_SQL_Server)
            //{
            //    stringBuilder.AppendLine(string.Format("/*dialect*/ MERGE INTO {0} T0 USING\r\n                           (\r\n                       SELECT FID,STUFF((SELECT ',' + FADVANCENO FROM (SELECT DISTINCT TSO.FID,TSOPE.FADVANCENO FROM {0} TM\r\n\t\t\t\t\t\t INNER JOIN T_SAL_ORDER TSO\r\n\t\t\t\t\t\t ON TSO.FID = TM.FID\r\n                         INNER JOIN T_SAL_ORDERPLAN TSOP\r\n                         ON TSO.FID = TSOP.FID AND TSOP.FNEEDRECADVANCE = 1\r\n                         INNER JOIN T_SAL_ORDERPLANENTRY TSOPE\r\n                         ON TSOPE.FENTRYID = TSOP.FENTRYID ) \r\n\t\t\t\t\t\t subTitle WHERE FID=A.FID FOR XML PATH('')),1, 1, '') AS FADVANCEBILLNO\r\n                         FROM (SELECT DISTINCT TSO.FID,TSOPE.FADVANCENO FROM {0} TM\r\n\t\t\t\t\t\t INNER JOIN T_SAL_ORDER TSO\r\n\t\t\t\t\t\t ON TSO.FID = TM.FID\r\n                         INNER JOIN T_SAL_ORDERPLAN TSOP\r\n                         ON TSO.FID = TSOP.FID AND TSOP.FNEEDRECADVANCE = 1\r\n                         INNER JOIN T_SAL_ORDERPLANENTRY TSOPE\r\n                         ON TSOPE.FENTRYID = TSOP.FENTRYID )A\r\n                         GROUP BY FID\r\n                          ) UT ON (T0.FID = UT.FID)\r\n                        WHEN MATCHED THEN UPDATE SET T0.FADVANCEBILLNO = UT.FADVANCEBILLNO;", rptTmpTable));
            //}
            //else
            //{
            //    stringBuilder.AppendLine(string.Format("/*dialect*/ MERGE INTO {0} T0 USING\r\n                (\r\n                   SELECT T.FID , SUBSTR(SYS_CONNECT_BY_PATH(T.FADVANCENO, ','), 2) AS FADVANCEBILLNO\r\n                  FROM (SELECT FID, FADVANCENO, ROW_NUMBER() OVER(PARTITION BY FID ORDER BY ROWNUM) RN\r\n                          FROM (SELECT DISTINCT TSO.FID, TSOPE.FADVANCENO\r\n                                  FROM {0} TM\r\n                                 INNER JOIN T_SAL_ORDER TSO\r\n                                    ON TSO.FID = TM.FID\r\n                                 INNER JOIN T_SAL_ORDERPLAN TSOP\r\n                                    ON TSO.FID = TSOP.FID\r\n                                   AND TSOP.FNEEDRECADVANCE = 1\r\n                                 INNER JOIN T_SAL_ORDERPLANENTRY TSOPE\r\n                                    ON TSOPE.FENTRYID = TSOP.FENTRYID) NT) T\r\n                 WHERE CONNECT_BY_ISLEAF = 1\r\n                 START WITH RN = 1\r\n                CONNECT BY RN = PRIOR RN + 1\r\n                       AND FID = PRIOR FID\r\n                )UT ON (T0.FID = UT.FID)\r\n                 WHEN MATCHED THEN UPDATE SET T0.FADVANCEBILLNO = UT.FADVANCEBILLNO", rptTmpTable));
            //}
            //list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            //stringBuilder.Clear();
            //stringBuilder.AppendLine(string.Format("MERGE INTO {0} TT Using\r\n                                    ( SELECT FSEQ FROM  {0} MT \r\n                                        WHERE NOT EXISTS \r\n                                        (SELECT 1 FROM (SELECT FID, MIN(FSEQ) FSEQ FROM {0} GROUP BY FID) GT where GT.FSEQ=MT.FSEQ)\r\n                                    ) UT ON (TT.FSEQ=UT.FSEQ) \r\n                                    When Matched Then Update Set FADVANCEBILLNO = NULL", rptTmpTable));
            //list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            //stringBuilder.Clear();
            //stringBuilder.AppendLine(string.Format("MERGE INTO {0} TT Using\r\n                                    ( SELECT FSEQ,FISRECMAT FROM  {0} MT \r\n                                        WHERE NOT EXISTS \r\n                                        (SELECT 1 FROM (SELECT FID, MIN(FSEQ) FSEQ FROM {0} GROUP BY FID) GT where GT.FSEQ=MT.FSEQ)\r\n                                    ) UT ON (TT.FSEQ=UT.FSEQ AND TT.FISRECMAT = 0) \r\n                                    When Matched Then Update Set FADVANCEAMOUNT = 0", rptTmpTable));
            //list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            //stringBuilder.Clear();
            stringBuilder.AppendLine(string.Format("Merge Into {0} TT Using\r\n                            ( SELECT FSEQ FROM {0} MT \r\n                                WHERE NOT EXISTS \r\n                                (SELECT 1 FROM (SELECT FORDERID, MIN(FSEQ) FSEQ FROM {0} GROUP BY FOrderId) GT where GT.FSEQ=MT.FSEQ)\r\n                            ) UT ON (TT.FSEQ=UT.FSEQ)\r\n                            When Matched Then Update Set FORDERNAME=NULL,FDELIVERYDAY = NULL,FMATERIALNUMBER = NULL,FMAPNUMBER = NULL,FMAPNAME = NULL,FSPECIFICATION=NULL,FMATERIALNAME = NULL \r\n                            ,FMATERIALGROUPNAME = NULL,FUNIT = NULL,FPRICEUNIT = NULL,FPRICE = NULL,FCHARGE=NULL,FORDERCHARGEQTY=NULL,FMONEYTYPE = NULL", rptTmpTable));
            list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
            //stringBuilder.AppendLine(string.Format("Merge Into {0} TT Using\r\n                            ( SELECT FSEQ FROM  {0} MT \r\n                                WHERE NOT EXISTS \r\n                                (SELECT 1 FROM (SELECT FDELIID, MIN(FSEQ) FSEQ FROM {0} GROUP BY FOrderId,FDELIID) GT where GT.FSEQ=MT.FSEQ)\r\n                            ) UT ON (TT.FSEQ=UT.FSEQ)\r\n                            When Matched Then Update Set FDELIBILLNO=NULL", rptTmpTable));
            //list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            //stringBuilder.Clear();
            stringBuilder.AppendLine(string.Format("Merge Into {0} TT Using\r\n                            ( SELECT FSEQ FROM  {0} MT \r\n                                WHERE NOT EXISTS \r\n                                (SELECT 1 FROM (SELECT FOUTID, MIN(FSEQ) FSEQ FROM {0} GROUP BY FOrderId,FOUTID) GT where GT.FSEQ=MT.FSEQ)\r\n                             ) UT ON (TT.FSEQ=UT.FSEQ)\r\n                            When Matched Then Update Set FOUTBILLNO=NULL", rptTmpTable));
            list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
            //stringBuilder.AppendLine(string.Format("Merge Into {0} TT Using\r\n                            ( SELECT FSEQ FROM  {0} MT \r\n                                WHERE NOT EXISTS \r\n                                (SELECT 1 FROM (SELECT FRETURNPRDID, MIN(FSEQ) FSEQ FROM {0} GROUP BY FOrderId,FDELIID,FOUTID,FRETURNPRDID) GT where GT.FSEQ=MT.FSEQ)\r\n                            ) UT ON (TT.FSEQ=UT.FSEQ)\r\n                            When Matched Then Update Set FRETURNPRDBILLNO=NULL", rptTmpTable));
            //list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            //stringBuilder.Clear();
            //stringBuilder.AppendLine(string.Format("Merge Into {0} TT Using\r\n                            ( SELECT FSEQ FROM {0} MT \r\n                                WHERE NOT EXISTS \r\n                                (SELECT 1 FROM (SELECT FRETURNID, MIN(FSEQ) FSEQ FROM {0} GROUP BY FOrderId,FDELIID,FOUTID,FRETURNPRDID,FRETURNID) GT where GT.FSEQ=MT.FSEQ)\r\n                             ) UT ON (TT.FSEQ=UT.FSEQ)\r\n                            When Matched Then Update Set FRETURNBILLNO=NULL", rptTmpTable));
            //list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            //stringBuilder.Clear();
            if (!string.IsNullOrWhiteSpace(this.priceFrom) && this.priceFrom.Equals("RECEIVEBILL", StringComparison.CurrentCultureIgnoreCase))
            {
                stringBuilder.Clear();
                stringBuilder.AppendFormat("\r\n                                    merge into {0} ts \r\n                                    using\r\n                                    (\r\n                                        select tsd.FORDERID forderid,\r\n                                        max(recard.fentryid) frecid,\r\n                                        max(recardo.fsalunitid) funitid,\r\n                                        (case when sum(recardo.FSalQTY)=0 then  convert(decimal(18,8),sum(recard.FALLAMOUNTFOR))\r\n                                        else  convert(decimal(18,8),sum(recard.FALLAMOUNTFOR))/sum(recardo.FSalQTY) end) as fprice,\r\n                                        (case when sum(recard.FPRICEQTY)=0 then   convert(decimal(18,8),sum( recard.FALLAMOUNTFOR))\r\n                                             else convert(decimal(18,8),sum( recard.FALLAMOUNTFOR))/sum(recard.FPRICEQTY)  end) as fcharge,--处理精度不准的问题\r\n                                        max(recar.fcurrencyid) fcurrencyid ,\r\n                                        max(tcur.FNAME) fcurrencyname\r\n                                       \r\n                                        from T_AR_RECEIVABLE recar\r\n                                        inner join t_ar_receivableentry recard on recar.fid=recard.fid \r\n                                        inner join t_ar_receivableentry_o recardo on recard.fentryid=recardo.fentryid\r\n                                        inner join {1} tsd on tsd.frecid = recard.fentryid\r\n                                        left join T_BD_CURRENCY_L tcur on tcur.FCURRENCYID=recar.fcurrencyid and tcur.FLOCALEID={2}\r\n                                        where recard.ftaxprice<>0\r\n                                        group by  tsd.FORDERID\r\n                                    ) td on (td.FORDERID=ts.FORDERID) --如果一行订单对应的多张应收单，那么默认取第一行\r\n                                    when matched then update set ts.FCURRENCYID=td.fcurrencyid,ts.FMONEYTYPE=td.fcurrencyname,\r\n                                    ts.FRECUNITID=td.funitid,ts.fprice=td.fprice,ts.fcharge=td.fcharge  ", rptTmpTable, this.flowTable, base.Context.UserLocale.LCID);
                list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            }
            DBUtils.ExecuteBatchWithTime(base.Context, list, 300);
        }

        /// <summary>
        /// 获取源头数据
        /// </summary>
        /// <param name="rptFilterTable"></param>
        public void GetDataSource(string rptFilterTable)
        {
            string entityKey = "FSaleOrderEntry";
            long[] entryIds = SalRptCommon.GetEntryIds(base.Context, rptFilterTable, this.moreFilter, out this.filterSql);
            List<string> list = new List<string>();
            if (entryIds.Length > 0)
            {
                BusinessFlowDataService businessFlowDataService = new BusinessFlowDataService();
                BuildBFTrackerTempTableArgs buildBFTrackerTempTableArgs = new BuildBFTrackerTempTableArgs("SAL_SaleOrder", entityKey, entryIds);
                buildBFTrackerTempTableArgs.OnTime = true;
                buildBFTrackerTempTableArgs.IsInt64 = true;
                string text = this.ReadEntryTableName("SAL_SaleOrder", "FSaleOrderEntry");
                buildBFTrackerTempTableArgs.FirstTableName = text;
                buildBFTrackerTempTableArgs.TableNames.Add(text);

                // buildBFTrackerTempTableArgs.TableNames.Add(this.ReadEntryTableName("SAL_DELIVERYNOTICE", "FEntity"));
                buildBFTrackerTempTableArgs.TableNames.Add(this.ReadEntryTableName("SAL_OUTSTOCK", "FEntity"));
                // buildBFTrackerTempTableArgs.TableNames.Add(this.ReadEntryTableName("STK_InStock", "FInStockEntry"));
                // buildBFTrackerTempTableArgs.TableNames.Add(this.ReadEntryTableName("PUR_MRB", "FPURMRBENTRY"));
                //buildBFTrackerTempTableArgs.TableNames.Add(this.ReadEntryTableName("STK_TRANSFEROUT", "FSTKTRSOUTENTRY"));
                //buildBFTrackerTempTableArgs.TableNames.Add(this.ReadEntryTableName("STK_TRANSFERIN", "FSTKTRSINENTRY"));
                //buildBFTrackerTempTableArgs.TableNames.Add(this.ReadEntryTableName("SAL_RETURNNOTICE", "FEntity"));
                //buildBFTrackerTempTableArgs.TableNames.Add(this.ReadEntryTableName("SAL_RETURNSTOCK", "FEntity"));
                //buildBFTrackerTempTableArgs.TableNames.Add(this.ReadEntryTableName("AR_receivable", "FEntityDetail"));
                buildBFTrackerTempTableArgs.TableNames.Add(this.ReadEntryTableName("SAL_INITOUTSTOCK", "FEntity"));
                buildBFTrackerTempTableArgs.TableNames.Add(this.ReadEntryTableName("PRD_MO", "FTreeEntity"));
                buildBFTrackerTempTableArgs.TableNames.Add(this.ReadEntryTableName("PRD_INSTOCK", "FEntity"));
                buildBFTrackerTempTableArgs.TableNames.Add(this.ReadEntryTableName("PUR_PurchaseOrder", "FPOOrderEntry"));
                buildBFTrackerTempTableArgs.TableNames.Add(this.ReadEntryTableName("STK_InStock", "FInStockEntry"));
      

                //  buildBFTrackerTempTableArgs.TableNames.Add(this.ReadEntryTableName("SFC_OperationPlanning", "FEntity"));


                BuildBFTrackerTempTableResult buildBFTrackerTempTableResult = businessFlowDataService.BuildBFTrackerTempTable(base.Context, buildBFTrackerTempTableArgs);
                if (buildBFTrackerTempTableResult != null)
                {
                    string tempTableName = buildBFTrackerTempTableResult.TempTableName;
                    this.deleteTables.Add(tempTableName);
                    foreach (string current in buildBFTrackerTempTableResult.TableColumns)
                    {
                        if (buildBFTrackerTempTableArgs.TableNames.Contains(current) && !list.Contains(current))
                        {
                            list.Add(current);
                        }
                    }
                    SalRptCommon.GetNeedDataByFields(base.Context, list, tempTableName, this.flowDataTable, entryIds);
                    //this.InsertFlowData(list, this.flowDataTable, tempTableName);
                    this.deleteTables.Add(this.flowDataTable);
                    SalRptCommon.InsertFlowData(base.Context, this.flowTable, list, this.flowDataTable, this.filterSql, entryIds, this.bIncludedUnfilledOrders);
                    //SalRptCommon.InsertReceivableData(base.Context, this.receivableHelpTable, this.receivableTable, this.flowTable);
                    //SalRptCommon.InsertInvoceData(base.Context, this.invoceTable, this.flowTable);
                    return;
                }
                SalRptCommon.InsertCustomFlowData(base.Context, this.flowTable, rptFilterTable, this.moreFilter);
            }
        }
        private void InsertFlowData(List<string> lstTable, string flowDataTable, string flowData)
        {
            string sql = string.Empty;
            List<SqlObject> list = new List<SqlObject>();
            if (lstTable.Contains("T_SAL_ORDERENTRY") && lstTable.Contains("t_AR_receivableEntry"))
            {
                sql = string.Format("insert into {0} (T_SAL_ORDERENTRY,t_AR_receivableEntry,t_AR_receivableEntry_SID,FIDENTITYID)\r\n                                SELECT FORDERENTRYID,FENTRYID,FSRCROWID,0 as FIDENTITYID from\r\n                                (\r\n                                SELECT TARE.FORDERENTRYID,TARE.FENTRYID,TARE.FSRCROWID FROM T_AR_RECEIVABLEENTRY TARE LEFT JOIN T_AR_RECEIVABLE TAR ON TARE.FID=TAR.FID WHERE EXISTS(\r\n                                select 1 from T_AR_RECEIVABLEENTRY TARS  INNER JOIN  {1} T1 ON TARS.FENTRYID=T1.t_AR_receivableEntry \r\n                                WHERE TAR.FBILLNO=TARS.FSOURCEBILLNO AND TARS.FSOURCETYPE='AR_receivable')\r\n                                UNION ALL\r\n                                SELECT TARE2.FORDERENTRYID,TARE2.FENTRYID,TARE2.FSRCROWID FROM T_AR_RECEIVABLEENTRY TARE2 WHERE EXISTS\r\n                                (SELECT 1 FROM T_AR_RECEIVABLEENTRY TARE1 LEFT JOIN T_AR_RECEIVABLE TAR ON TARE1.FID=TAR.FID  WHERE EXISTS(\r\n                                select 1 from T_AR_RECEIVABLEENTRY TARS  INNER JOIN  {1} T2 ON TARS.FENTRYID=T2.t_AR_receivableEntry \r\n                                WHERE TAR.FBILLNO=TARS.FSOURCEBILLNO AND TARS.FSOURCETYPE='AR_receivable') AND TARE2.FSRCROWID=TARE1.FENTRYID)\r\n                                ) T", flowDataTable, flowData);
                list.Add(new SqlObject(sql, new List<SqlParam>()));
                DBUtils.ExecuteBatchWithTime(base.Context, list, 300);
            }
        }
        private string ReadEntryTableName(string formId, string entityKey = "")
        {
            string text = entityKey;
            if (string.IsNullOrWhiteSpace(text))
            {
                FormMetadata formMetadata = ServiceHelper.GetService<IMetaDataService>().Load(base.Context, formId, true) as FormMetadata;
                BusinessInfo businessInfo = formMetadata.BusinessInfo;
                text = businessInfo.GetForm().LinkSet.LinkEntitys[0].ParentEntityKey;
            }
            IBusinessFlowService service = ServiceHelper.GetService<IBusinessFlowService>();
            TableDefine tableDefine = service.LoadTableDefine(base.Context, formId, text);
            if (tableDefine == null)
            {
                return string.Empty;
            }
            return tableDefine.TableNumber;
        }
        protected bool IsCanViewAmount(IRptParams filter)
        {
            IPermissionService permissionService = Kingdee.BOS.Contracts.ServiceFactory.GetPermissionService(base.Context);
            PermissionAuthResult permissionAuthResult = permissionService.FuncPermissionAuth(base.Context, new BusinessObject
            {
                Id = "JHF_SalProductDetailRpt"
            }, "1ff7fbd3148440809820b8f039a228a9");
            Kingdee.BOS.Contracts.ServiceFactory.CloseService(permissionService);
            if (!permissionAuthResult.Passed)
            {
                return false;
            }
            string text;
            if (filter.CustomParams.ContainsKey("ParentReportCurrentRow"))
            {
                Dictionary<string, object> dictionary = filter.CustomParams["ParentReportCurrentRow"] as Dictionary<string, object>;
                text = Convert.ToString(dictionary["FSALEORGID"]);
            }
            else
            {
                text = this.GetDataByKey(filter.FilterParameter.CustomFilter, "SaleOrgList");
            }
            if (text.IsNullOrEmptyOrWhiteSpace())
            {
                return true;
            }
            string[] array = text.Split(new char[]
            {
                ','
            });
            if (permissionAuthResult.OrgActions.IsNullOrEmpty() || permissionAuthResult.OrgActions.Count == 0)
            {
                return false;
            }
            List<long> list = (
                from p in permissionAuthResult.OrgActions
                select p.Id).ToList<long>();
            string[] array2 = array;
            for (int i = 0; i < array2.Length; i++)
            {
                string value = array2[i];
                if (!value.IsNullOrEmptyOrWhiteSpace() && !list.Contains(Convert.ToInt64(value)))
                {
                    return false;
                }
            }
            return true;
        }
        private void InitTables()
        {
            if (this.deleteTables != null && this.deleteTables.Count > 0)
            {
                SalRptCommon.DropTempTables(base.Context, this.deleteTables);
                this.deleteTables.Clear();
            }
            List<SqlObject> list = new List<SqlObject>();
            this.rptFilterTable = SalRptCommon.AddTempTable(base.Context);
            //高级过滤临时表
            SalRptCommon.CreateAdvanceTable(base.Context, this.rptFilterTable, list);
            this.deleteTables.Add(this.rptFilterTable);
            this.tmpRptTbl = SalRptCommon.AddTempTable(base.Context);
            //最终临时表
            this.CreateTmpRptTable(this.tmpRptTbl, list);
            this.deleteTables.Add(this.tmpRptTbl);
            //流程数据表
            this.flowTable = SalRptCommon.AddTempTable(base.Context);
            this.deleteTables.Add(this.flowTable);
            SalRptCommon.CreateFlowTable(base.Context, this.flowTable, list);
            //this.receivableHelpTable = SalRptCommon.AddTempTable(base.Context);
            //this.deleteTables.Add(this.receivableHelpTable);
            //SalRptCommon.CreateReceivableHelpTable(this.receivableHelpTable, list);
            //this.receivableTable = SalRptCommon.AddTempTable(base.Context);
            //this.deleteTables.Add(this.receivableTable);
            //SalRptCommon.CreateReceivableTable(this.receivableTable, list);
            //this.receBillnoTable = SalRptCommon.AddTempTable(base.Context);
            //this.deleteTables.Add(this.receBillnoTable);
            //SalRptCommon.CreateRecBillnoTable(this.receBillnoTable, list);
            //this.invoceTable = SalRptCommon.AddTempTable(base.Context);
            //this.deleteTables.Add(this.invoceTable);
            //SalRptCommon.CreateInvoceTable(this.invoceTable, list);
            //this.invoceBillnoTable = SalRptCommon.AddTempTable(base.Context);
            //this.deleteTables.Add(this.invoceBillnoTable);
            //SalRptCommon.CreateInvoceBillnoTable(this.invoceBillnoTable, list);
            //this.invoceBillno_RTable = SalRptCommon.AddTempTable(base.Context);
            //this.deleteTables.Add(this.invoceBillno_RTable);
            //SalProductDetailRpt.CreateInvoceBillno_RTable(this.invoceBillno_RTable, list);
            //this.recAmountTable = SalRptCommon.AddTempTable(base.Context);
            //this.deleteTables.Add(this.recAmountTable);
            //this.CreateRecAmountTable(this.recAmountTable, list);
            this.flowDataTable = SalRptCommon.AddTempTable(base.Context);
            using (KDTransactionScope kDTransactionScope = new KDTransactionScope(TransactionScopeOption.RequiresNew))
            {
                DBUtils.ExecuteBatchWithTime(base.Context, list, 100);
                kDTransactionScope.Complete();
            }
        }
        public void CreateTmpRptTable(string tmpTable, List<SqlObject> lstTable)
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine(string.Format(" CREATE TABLE {0} ( ", this.tmpRptTbl));
            stringBuilder.AppendLine("     FID INT,");
            stringBuilder.AppendLine("     FORDERID INT ,");
            stringBuilder.AppendLine("     FORDERNAME nvarchar(100) null,");
            stringBuilder.AppendLine("     FSEQ INT ,");
            stringBuilder.AppendLine("     FSALEORGID int null,  ");
            stringBuilder.AppendLine("     FSALEORGNAME nvarchar(100) null ,");
            stringBuilder.AppendLine("     FBILLNO varchar(80) null ,  ");
            stringBuilder.AppendLine("     FBILLTYPE nvarchar(100) null ,  ");
            stringBuilder.AppendLine("     FDATE datetime  null,  ");
            stringBuilder.AppendLine("     FSALEDEPTID int null,  ");
            stringBuilder.AppendLine("     FSALEDEPT  nvarchar(255) null,  ");
            stringBuilder.AppendLine("     FSALEGROUPID int null,  ");
            stringBuilder.AppendLine("     FSALEGROUP  nvarchar(255) null, ");
            stringBuilder.AppendLine("    FSALERID int null,  ");
            stringBuilder.AppendLine("     FSALES  nvarchar(255) null, ");
            stringBuilder.AppendLine("     FCUSTID INT NULL, ");
            stringBuilder.AppendLine("     FCUSTOMERNUMBER nvarchar(100) null,  ");
            stringBuilder.AppendLine("     FCUSTOMERNAME nvarchar(255)  null, ");
            stringBuilder.AppendLine("     FDELIVERYDAY  datetime null , ");
            stringBuilder.AppendLine("     FMATERIALID  INT  null, ");
            stringBuilder.AppendLine("     FMATERIALNUMBER  nvarchar(100)  null, ");
            stringBuilder.AppendLine("     FMATERIALNAME  nvarchar(255)  null, ");
            stringBuilder.AppendLine("     FMAPID  nvarchar(100)  null, ");
            stringBuilder.AppendLine("     FMAPNUMBER  nvarchar(255)  null, ");
            stringBuilder.AppendLine("     FMAPNAME  nvarchar(255)  null, ");
            stringBuilder.AppendLine("     FMATERIALGROUP  int  null , ");
            stringBuilder.AppendLine("     FMATERIALGROUPNAME nvarchar(100)  null , ");
            stringBuilder.AppendLine("     FSPECIFICATION nvarchar(510) null, ");
            stringBuilder.AppendLine("     FAuxPropId int default 0, ");
            stringBuilder.AppendLine("     FAuxProp nvarchar(1500) null,  ");
            stringBuilder.AppendLine("     FUNITID  int  not null default (0), ");
            stringBuilder.AppendLine("     FUNIT  nvarchar(100) null, ");
            stringBuilder.AppendLine("     FPRICEUNITID  int  not null default (0), ");
            stringBuilder.AppendLine("     FPRICEUNIT  nvarchar(100) null, ");
            stringBuilder.AppendLine("     FCURRENCYID INT NULL,  ");
            stringBuilder.AppendLine("     FMONEYTYPE nvarchar(100) null,");
            stringBuilder.AppendLine("     FPRICE  decimal(24,10)  null , ");
            stringBuilder.AppendLine("     FCHARGE  decimal(24,10)  null , ");
            stringBuilder.AppendLine("     FORDERQTY  decimal(24,10)  null , ");
            stringBuilder.AppendLine("     FORDERCHARGEQTY  decimal(24,10)  null , ");
            stringBuilder.AppendLine("     FORDERAMOUNT  decimal(24,10)  null , ");
            //stringBuilder.AppendLine("     FDELIID int null, ");
            //stringBuilder.AppendLine("     FDELIBILLNO  nvarchar(100)  null, ");
            //stringBuilder.AppendLine("     FDELIQTY decimal(24,10)  null , ");
            //stringBuilder.AppendLine("     FDELIPRICEUNITID  int  null , ");
            //stringBuilder.AppendLine("     FDELICHARGEQTY  decimal(24,10)  null , ");
            //stringBuilder.AppendLine("     FDELIAMOUNT  decimal(24,10)  null , ");
            stringBuilder.AppendLine("     FOUTID int null, ");
            stringBuilder.AppendLine("     FOUTBILLNO nvarchar(100)  null , ");
            stringBuilder.AppendLine("     FOUTQTY  decimal(24,10)  null , ");
            stringBuilder.AppendLine("     FOUTPRICEUNITID  int  null , ");
            stringBuilder.AppendLine("     FOUTCHARGEQTY  decimal(24,10)  null , ");
            stringBuilder.AppendLine("     FOUTAMOUNT  decimal(24,10)   null , ");
            stringBuilder.AppendLine("     FMOID int null, ");
            stringBuilder.AppendLine("     FMOBILLNO nvarchar(100) null, ");
            stringBuilder.AppendLine("     FMODATE datetime  null, ");
            stringBuilder.AppendLine("     FMOQTY  decimal(24,10)  null , ");
            stringBuilder.AppendLine("     FPRDINSTOCKID int null, ");
            stringBuilder.AppendLine("     FMOINSTOCKDATE  datetime  null , ");
            stringBuilder.AppendLine("     FMOINSTOCKQTY  decimal(24,10)  null , ");

            stringBuilder.AppendLine("     FPURORDERID int null, ");
            stringBuilder.AppendLine("     FPURORDERNO nvarchar(100) null, ");
            stringBuilder.AppendLine("     FPURORDERDATE datetime  null, ");
            stringBuilder.AppendLine("     FPURORDERQTY  decimal(24,10)  null , ");
            stringBuilder.AppendLine("     FPURINSTOCKID  int  null , ");
            stringBuilder.AppendLine("     FPURINSTOCKDATE  datetime  null , ");
            stringBuilder.AppendLine("     FPURINSTOCKQTY  decimal(24,10)  null , ");



            stringBuilder.AppendLine("     FOPEID int null, ");
            stringBuilder.AppendLine("     FOPENO nvarchar(100) null, ");
            stringBuilder.AppendLine("     FOPEDATE datetime  null, ");
            stringBuilder.AppendLine("     FOPERROUTENAME  nvarchar(100)  null , ");
            stringBuilder.AppendLine("     FOPEQTY  decimal(24,10)  null , ");
            stringBuilder.AppendLine("     FENDOPEDATE  datetime  null , ");
            stringBuilder.AppendLine("     FENDQTY   decimal(24,10)  null , ");



            stringBuilder.AppendLine("     FPLANID int null, ");
            stringBuilder.AppendLine("     FPLANNO nvarchar(100) null, ");
            stringBuilder.AppendLine("     FPLANDATE datetime  null, ");
            stringBuilder.AppendLine("     FPLANQTY  decimal(24,10)  null , ");

            //stringBuilder.AppendLine("     FRETURNPRDID int null, ");
            //stringBuilder.AppendLine("     FRETURNPRDBILLNO nvarchar(100) null, ");
            //stringBuilder.AppendLine("     FRETURNPQTY  decimal(24,10)  null , ");
            //stringBuilder.AppendLine("     FRETURNPPRICEUNITID  int  null , ");
            //stringBuilder.AppendLine("     FRETURNPCHARGEQTY  decimal(24,10)  null , ");
            //stringBuilder.AppendLine("     FRETURNPAMOUNT  decimal(24,10)  null , ");
            //stringBuilder.AppendLine("     FRETURNID int null, ");
            //stringBuilder.AppendLine("     FRETURNBILLNO nvarchar(100)  null, ");
            //stringBuilder.AppendLine("     FRETURNQTY  decimal(24,10)  null , ");
            //stringBuilder.AppendLine("     FRETURNPRICEUNITID  int  null , ");
            //stringBuilder.AppendLine("     FRETURNCHARGEQTY  decimal(24,10)  null , ");
            //stringBuilder.AppendLine("     FRETURNAMOUNT  decimal(24,10)  null , ");
            //stringBuilder.AppendLine("     FRECENTRYID int null, ");
            //stringBuilder.AppendLine("      FRECEIVEBILLNO  nvarchar(100)  null , ");
            //stringBuilder.AppendLine("     FRECQTY Decimal(24,10) null, ");
            //stringBuilder.AppendLine("     FRECEPRICEUNITID int null, ");
            //stringBuilder.AppendLine("     FRECEIVECHARGEQTY  decimal(24,10)  null , ");
            //stringBuilder.AppendLine("     FRECAMOUNT  Decimal(24,10)  null , ");
            //stringBuilder.AppendLine("     FWRITEOFFAMOUNT  Decimal(24,10)  null , ");
            //stringBuilder.AppendLine("     FSETACCOUNTTYPE  nvarchar(100)  null , ");
            //stringBuilder.AppendLine("     FINVOECEBILLNO  nvarchar(1000)  null , ");
            //stringBuilder.AppendLine("     FINVOECEQTY  decimal(24,10) null , ");
            //stringBuilder.AppendLine("     FBILLCHARGEQTY  decimal(24,10)  null , ");
            //stringBuilder.AppendLine("     FINVOECEAMOUNT  decimal(24,10)  null ,  ");
            //stringBuilder.AppendLine("     FRECBILLNO  nvarchar(1000)  null, ");
            //stringBuilder.AppendLine("     FCHARGEOFFBILLNO  nvarchar(1000)  null, ");
            //stringBuilder.AppendLine("     FADVANCEBILLNO  nvarchar(1000)  null, ");
            //stringBuilder.AppendLine("     FADVANCEAMOUNT  decimal(24,10)  null,");
            //stringBuilder.AppendLine("     FRECEIPTAMOUNT  decimal(24,10)  null , ");
            //stringBuilder.AppendLine("     FNRECEIPTAMOUNT  decimal(24,10)  null , ");
            //stringBuilder.AppendLine("     FRECSUMAMOUNNT decimal(24,10)  null , ");
            //stringBuilder.AppendLine("     FCHARGEOFFAMOUNT decimal(24,10)  null , ");
            //stringBuilder.AppendLine("     FRECBILLID INT  null , ");
            stringBuilder.AppendLine("     FPRECISION int null default(0),");
            stringBuilder.AppendLine("     FCHARGEPRECISION int null default(0),");
            stringBuilder.AppendLine("     FPRICEDIGITS int null default(0),");
            stringBuilder.AppendLine("     FAMOUNTDIGITS int null default(0),");
            stringBuilder.AppendLine("     FBASEUNITID_ID  int  not null default (0), ");
            stringBuilder.AppendLine("     FBASEUNITID nvarchar(100) null,");
            stringBuilder.AppendLine("     FORDERBASEQTY decimal(24,10) null default(0),");
            stringBuilder.AppendLine("     FBASEPRICE decimal(24,10) null default(0),");
            //stringBuilder.AppendLine("     FDELIVERYNOTICEQTY decimal(24,10) null default(0),");
            //stringBuilder.AppendLine("     FDELIVERYQTY decimal(24,10) null default(0),");
            //stringBuilder.AppendLine("     FRETURNNOTICEQTY decimal(24,10) null default(0),");
            //stringBuilder.AppendLine("     FRETURNBASEQTY decimal(24,10) null default(0),");
            //stringBuilder.AppendLine("     FRECEIVEQTY decimal(24,10) null default(0),");
            //stringBuilder.AppendLine("     FBASEINVOICEQTY decimal(24,10) null default(0),");
            stringBuilder.AppendLine("     FCLOSESTATUS  nvarchar(10)  null , ");
            stringBuilder.AppendLine("     FMRPCLOSESTATUS  nvarchar(10)  null , ");
            stringBuilder.AppendLine("     FMRPTERMINATESTATUS  nvarchar(10)  null , ");
            //stringBuilder.AppendLine("     FRECUNITID  INT  NOT null  DEFAULT (0), ");
            stringBuilder.AppendLine("     FISRECMAT  char(1)  null ");
            stringBuilder.AppendLine(" ) ");
            lstTable.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
        }
        private void SetFieldDecimal(string controlFiledName, string destFieldName)
        {
            DecimalControlField item = new DecimalControlField
            {
                ByDecimalControlFieldName = controlFiledName,
                DecimalControlFieldName = destFieldName
            };
            this.lstDcf.Add(item);
        }
        protected void SetDecimalControl()
        {
            this.SetFieldDecimal("FPRICE", "FPRICEDIGITS");
            this.SetFieldDecimal("FCHARGE", "FPRICEDIGITS");
            this.SetFieldDecimal("FORDERQTY", "FPRECISION");
            this.SetFieldDecimal("FORDERCHARGEQTY", "FCHARGEPRECISION");
            this.SetFieldDecimal("FORDERAMOUNT", "FAMOUNTDIGITS");
            //this.SetFieldDecimal("FDELIQTY", "FPRECISION");
            //this.SetFieldDecimal("FDELICHARGEQTY", "FCHARGEPRECISION");
            //this.SetFieldDecimal("FDELIAMOUNT", "FAMOUNTDIGITS");
            this.SetFieldDecimal("FOUTQTY", "FPRECISION");
            this.SetFieldDecimal("FOUTCHARGEQTY", "FCHARGEPRECISION");
            this.SetFieldDecimal("FOUTAMOUNT", "FAMOUNTDIGITS");
            //this.SetFieldDecimal("FRETURNPQTY", "FPRECISION");
            //this.SetFieldDecimal("FRETURNPCHARGEQTY", "FCHARGEPRECISION");
            //this.SetFieldDecimal("FRETURNPAMOUNT", "FAMOUNTDIGITS");
            //this.SetFieldDecimal("FRETURNQTY", "FPRECISION");
            //this.SetFieldDecimal("FRETURNCHARGEQTY", "FCHARGEPRECISION");
            //this.SetFieldDecimal("FRETURNAMOUNT", "FAMOUNTDIGITS");
            //this.SetFieldDecimal("FINVOECEQTY", "FPRECISION");
            //this.SetFieldDecimal("FBILLCHARGEQTY", "FCHARGEPRECISION");
            //this.SetFieldDecimal("FINVOECEAMOUNT", "FAMOUNTDIGITS");
            //this.SetFieldDecimal("FRECQTY", "FPRECISION");
            //this.SetFieldDecimal("FRECEIVECHARGEQTY", "FCHARGEPRECISION");
            //this.SetFieldDecimal("FRECAMOUNT", "FAMOUNTDIGITS");
            //this.SetFieldDecimal("FWRITEOFFAMOUNT", "FAMOUNTDIGITS");
            //this.SetFieldDecimal("FRECEIPTAMOUNT", "FAMOUNTDIGITS");
            //this.SetFieldDecimal("FNRECEIPTAMOUNT", "FAMOUNTDIGITS");
            //this.SetFieldDecimal("FRECSUMAMOUNNT", "FAMOUNTDIGITS");
            //this.SetFieldDecimal("FADVANCEAMOUNT", "FAMOUNTDIGITS");
            //this.SetFieldDecimal("FCHARGEOFFAMOUNT", "FAMOUNTDIGITS");
            base.ReportProperty.DecimalControlFieldList = this.lstDcf;
        }
        public string GetFiledData()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("  FORDERID,FORDERNAME");
            stringBuilder.AppendLine("  ,FSALEORGID,FSALEORGNAME,FBILLNO,FBILLTYPE,FDATE, FSALEDEPTID,FSALEDEPT");
            stringBuilder.AppendLine(" ,FSALEGROUPID ,FSALEGROUP,FSALERID,FSALES");
            stringBuilder.AppendLine(" ,FCUSTID,FCUSTOMERNUMBER,FCUSTOMERNAME,FDELIVERYDAY");
            stringBuilder.AppendLine(" ,FMATERIALID,FMATERIALNUMBER,FMATERIALNAME,FMATERIALGROUP,FMATERIALGROUPNAME ");
            stringBuilder.AppendLine(" ,FMAPID,FMAPNUMBER,FMAPNAME");
            stringBuilder.AppendLine(" ,FSPECIFICATION,FAuxPropId,FAuxProp  ");
            stringBuilder.AppendLine(" ,FUNIT,FPRICEUNIT,FCURRENCYID,FMONEYTYPE,FPRICE,FCHARGE");
            stringBuilder.AppendLine(" ,FORDERQTY,FORDERCHARGEQTY,FORDERAMOUNT--,FDELIBILLNO,FDELIQTY,FDELICHARGEQTY,FDELIAMOUNT");
            stringBuilder.AppendLine(" ,FOUTBILLNO,FOUTQTY,FOUTCHARGEQTY,FOUTAMOUNT");


            stringBuilder.AppendLine(" ,FMOBILLNO,FMODATE,FMOQTY,FMOINSTOCKDATE");
            stringBuilder.AppendLine(" ,FMOINSTOCKQTY,FPURORDERNO,FPURORDERDATE,FPURORDERQTY");

            stringBuilder.AppendLine(" ,FPURINSTOCKDATE,FPURINSTOCKQTY");


            stringBuilder.AppendLine(" ,FOPENO,FOPEDATE,FOPERROUTENAME,FOPEQTY,FENDOPEDATE,FENDQTY");
            stringBuilder.AppendLine(" ,FPLANNO,FPLANDATE,FPLANQTY");
            
            // stringBuilder.AppendLine(" ,FRETURNPRDBILLNO,FRETURNPQTY,FRETURNPCHARGEQTY,FRETURNPAMOUNT");
            //stringBuilder.AppendLine(" ,FRETURNBILLNO,FRETURNQTY,FRETURNCHARGEQTY,FRETURNAMOUNT");
            //stringBuilder.AppendLine(" ,FRECEIVEBILLNO,FRECQTY,FRECEIVECHARGEQTY,FRECAMOUNT,FWRITEOFFAMOUNT,FSETACCOUNTTYPE");
            //stringBuilder.AppendLine(" ,FINVOECEBILLNO,FINVOECEQTY,FBILLCHARGEQTY,FINVOECEAMOUNT");
            //stringBuilder.AppendLine(" ,FRECBILLNO,FCHARGEOFFBILLNO,FADVANCEBILLNO");
            //stringBuilder.AppendLine(" ,FRECEIPTAMOUNT,FNRECEIPTAMOUNT,FRECSUMAMOUNNT,FCHARGEOFFAMOUNT,FRECBILLID,FADVANCEAMOUNT ");
            stringBuilder.AppendLine(" ,FPRECISION,FCHARGEPRECISION,FPRICEDIGITS,FAMOUNTDIGITS");
            stringBuilder.AppendLine(" ,FBASEUNITID_ID,FBASEUNITID,FORDERBASEQTY,FBASEPRICE");
            //   stringBuilder.AppendLine("   ,FDELIVERYNOTICEQTY,FDELIVERYQTY,FRETURNNOTICEQTY,FRETURNBASEQTY,FRECEIVEQTY,FBASEINVOICEQTY");
            stringBuilder.AppendLine(" ,FCLOSESTATUS,FMRPCLOSESTATUS,FMRPTERMINATESTATUS--,FRECUNITID");
            stringBuilder.AppendLine("    ,FUNITID,FPRICEUNITID");

            return stringBuilder.ToString();
        }
        public void SetRptBosTable(string tableName)
        {
            List<SqlObject> list = new List<SqlObject>();
            StringBuilder stringBuilder = new StringBuilder();
            this.MergeOrderBillType(this.tmpRptTbl, list);
           // this.MergePriceQTY(list);
            this.KSQL_SEQ = string.Format(this.KSQL_SEQ, "FSEQ ");
            stringBuilder.AppendLine(" SELECT ");
            stringBuilder.AppendLine(this.GetFiledData());
            stringBuilder.AppendLine(string.Format(" , {0} INTO {1} FROM {2} ", this.KSQL_SEQ, tableName, this.tmpRptTbl));
            stringBuilder.AppendLine(" ");
            stringBuilder.AppendLine("  ");
            list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
            //bool flag = !string.IsNullOrWhiteSpace(this.priceFrom) && this.priceFrom.Equals("RECEIVEBILL", StringComparison.CurrentCultureIgnoreCase);
            //if (flag)
            //{
            //    list.Add(new SqlObject(this.GetMergeSQLForARSrc(tableName), new List<SqlParam>()));
            //}
            stringBuilder.AppendLine(string.Format(" TRUNCATE TABLE {0} ", this.tmpRptTbl));
            list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
            stringBuilder.AppendLine(string.Format(" TRUNCATE TABLE {0} ", this.rptFilterTable));
            list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
            stringBuilder.AppendLine(string.Format(" TRUNCATE TABLE {0} ", this.flowTable));
            list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
            //stringBuilder.AppendLine(string.Format(" TRUNCATE TABLE {0} ", this.receivableHelpTable));
            //list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            //stringBuilder.Clear();
            //stringBuilder.AppendLine(string.Format(" TRUNCATE TABLE {0} ", this.receivableTable));
            //list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            //stringBuilder.Clear();
            //stringBuilder.AppendLine(string.Format(" TRUNCATE TABLE {0} ", this.receBillnoTable));
            //list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            //stringBuilder.Clear();
            DBUtils.ExecuteBatchWithTime(base.Context, list, 300);
        }
        private void MergePriceQTY(List<SqlObject> lstSql)
        {
            StringBuilder stringBuilder = new StringBuilder();
            IUnitConvertService service = ServiceHelper.GetService<IUnitConvertService>();
            string strSQL = string.Format("SELECT * FROM {0} ST WHERE (ST.FOUTPRICEUNITID <> 0 AND ST.FPRICEUNITID <> ST.FOUTPRICEUNITID) \r\n                                                            OR (ST.FDELIPRICEUNITID <> 0 AND ST.FPRICEUNITID <> ST.FDELIPRICEUNITID) \r\n                                                            OR (ST.FRETURNPRICEUNITID <> 0 AND ST.FPRICEUNITID <> ST.FRETURNPRICEUNITID)\r\n                                                            OR (ST.FRETURNPPRICEUNITID<> 0 AND ST.FPRICEUNITID <> ST.FRETURNPPRICEUNITID)\r\n                                                            OR (ST.FRECEPRICEUNITID <> 0 AND ST.FPRICEUNITID <> ST.FRECEPRICEUNITID)", this.tmpRptTbl);
            DynamicObjectCollection dynamicObjectCollection = DBUtils.ExecuteDynamicObject(base.Context, strSQL, null, null, CommandType.Text, new SqlParam[0]);
            foreach (DynamicObject current in dynamicObjectCollection)
            {
                int num = Convert.ToInt32(current["FORDERID"]);
                int num2 = Convert.ToInt32(current["FSEQ"]);
                int num3 = Convert.ToInt32(current["FPRICEUNITID"]);
                int num4 = Convert.ToInt32(current["FOUTPRICEUNITID"]);
                int num5 = Convert.ToInt32(current["FDELIPRICEUNITID"]);
                int num6 = Convert.ToInt32(current["FRETURNPPRICEUNITID"]);
                int num7 = Convert.ToInt32(current["FRETURNPRICEUNITID"]);
                int num8 = Convert.ToInt32(current["FRECEPRICEUNITID"]);
                if (num4 != 0 && num3 != num4)
                {
                    string format = "MERGE INTO {0} ST USING  \r\n                        (\r\n\t                        SELECT IT.FORDERID,IT.FOUTCHARGEQTY*IT.RATE AS  FOUTCHARGEQTY\r\n                            FROM ({1}) IT WHERE IT.FORDERID= {2} AND IT.FSEQ = {3}\r\n                        )TT ON (ST.FORDERID = TT.FORDERID)\r\n                        WHEN MATCHED THEN UPDATE SET ST.FOUTCHARGEQTY = TT.FOUTCHARGEQTY WHERE ST.FSEQ={3}";
                    UnitConvertParam unitConvertParam = new UnitConvertParam(true)
                    {
                        TableName = this.tmpRptTbl,
                        TableNameAlias = "T1",
                        SrcUnitFieldName = "FOUTPRICEUNITID",
                        DstUntiFieldName = "FPRICEUNITID",
                        MaterialFieldName = "FMATERIALID",
                        TablePrimaryKey = "FID"
                    };
                    unitConvertParam.OtherFieldName = new List<string>
                    {
                        "FOUTCHARGEQTY",
                        "FORDERID",
                        "FSEQ"
                    };
                    stringBuilder.AppendLine(string.Format(format, new object[]
                    {
                        this.tmpRptTbl,
                        service.GetUnitConvertSql(unitConvertParam),
                        num,
                        num2
                    }));
                    lstSql.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
                    stringBuilder.Clear();
                }
                if (num6 != 0 && num3 != num6)
                {
                    string format2 = "MERGE INTO {0} ST USING  \r\n                        (\r\n\t                        SELECT IT.FORDERID,IT.FRETURNPCHARGEQTY*IT.RATE AS  FRETURNPCHARGEQTY\r\n                            FROM ({1}) IT WHERE IT.FORDERID= {2} AND IT.FSEQ = {3}\r\n                        )TT ON (ST.FORDERID = TT.FORDERID)\r\n                        WHEN MATCHED THEN UPDATE SET ST.FRETURNPCHARGEQTY = TT.FRETURNPCHARGEQTY WHERE ST.FSEQ={3}";
                    UnitConvertParam unitConvertParam2 = new UnitConvertParam(true)
                    {
                        TableName = this.tmpRptTbl,
                        TableNameAlias = "T1",
                        SrcUnitFieldName = "FRETURNPPRICEUNITID",
                        DstUntiFieldName = "FPRICEUNITID",
                        MaterialFieldName = "FMATERIALID",
                        TablePrimaryKey = "FID"
                    };
                    unitConvertParam2.OtherFieldName = new List<string>
                    {
                        "FRETURNPCHARGEQTY",
                        "FORDERID",
                        "FSEQ"
                    };
                    stringBuilder.AppendLine(string.Format(format2, new object[]
                    {
                        this.tmpRptTbl,
                        service.GetUnitConvertSql(unitConvertParam2),
                        num,
                        num2
                    }));
                    lstSql.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
                    stringBuilder.Clear();
                }
                if (num5 != 0 && num3 != num5)
                {
                    string format3 = "MERGE INTO {0} ST USING  \r\n                        (\r\n\t                        SELECT IT.FORDERID,IT.FDELICHARGEQTY*IT.RATE AS  FDELICHARGEQTY\r\n                            FROM ({1}) IT WHERE IT.FORDERID= {2} AND IT.FSEQ = {3}\r\n                        )TT ON (ST.FORDERID = TT.FORDERID)\r\n                        WHEN MATCHED THEN UPDATE SET ST.FDELICHARGEQTY = TT.FDELICHARGEQTY WHERE ST.FSEQ={3}";
                    UnitConvertParam unitConvertParam3 = new UnitConvertParam(true)
                    {
                        TableName = this.tmpRptTbl,
                        TableNameAlias = "T1",
                        SrcUnitFieldName = "FDELIPRICEUNITID",
                        DstUntiFieldName = "FPRICEUNITID",
                        MaterialFieldName = "FMATERIALID",
                        TablePrimaryKey = "FID"
                    };
                    unitConvertParam3.OtherFieldName = new List<string>
                    {
                        "FDELICHARGEQTY",
                        "FORDERID",
                        "FSEQ"
                    };
                    stringBuilder.AppendLine(string.Format(format3, new object[]
                    {
                        this.tmpRptTbl,
                        service.GetUnitConvertSql(unitConvertParam3),
                        num,
                        num2
                    }));
                    lstSql.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
                    stringBuilder.Clear();
                }
                if (num7 != 0 && num3 != num7)
                {
                    string format4 = "MERGE INTO {0} ST USING  \r\n                        (\r\n\t                        SELECT IT.FORDERID,IT.FRETURNCHARGEQTY*IT.RATE AS  FRETURNCHARGEQTY\r\n                            FROM ({1}) IT WHERE IT.FORDERID= {2} AND IT.FSEQ = {3}\r\n                        )TT ON (ST.FORDERID = TT.FORDERID)\r\n                        WHEN MATCHED THEN UPDATE SET ST.FRETURNCHARGEQTY = TT.FRETURNCHARGEQTY WHERE ST.FSEQ={3}";
                    UnitConvertParam unitConvertParam4 = new UnitConvertParam(true)
                    {
                        TableName = this.tmpRptTbl,
                        TableNameAlias = "T1",
                        SrcUnitFieldName = "FRETURNPRICEUNITID",
                        DstUntiFieldName = "FPRICEUNITID",
                        MaterialFieldName = "FMATERIALID",
                        TablePrimaryKey = "FID"
                    };
                    unitConvertParam4.OtherFieldName = new List<string>
                    {
                        "FRETURNCHARGEQTY",
                        "FORDERID",
                        "FSEQ"
                    };
                    stringBuilder.AppendLine(string.Format(format4, new object[]
                    {
                        this.tmpRptTbl,
                        service.GetUnitConvertSql(unitConvertParam4),
                        num,
                        num2
                    }));
                    lstSql.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
                    stringBuilder.Clear();
                }
                if (num8 != 0 && num3 != num8)
                {
                    string format5 = "MERGE INTO {0} ST USING  \r\n                        (\r\n\t                        SELECT IT.FORDERID,IT.FRECEIVECHARGEQTY*IT.RATE AS  FRECEIVECHARGEQTY\r\n                            FROM ({1}) IT WHERE IT.FORDERID= {2} AND IT.FSEQ = {3}\r\n                        )TT ON (ST.FORDERID = TT.FORDERID)\r\n                        WHEN MATCHED THEN UPDATE SET ST.FRECEIVECHARGEQTY = TT.FRECEIVECHARGEQTY WHERE ST.FSEQ={3}";
                    UnitConvertParam unitConvertParam5 = new UnitConvertParam(true)
                    {
                        TableName = this.tmpRptTbl,
                        TableNameAlias = "T1",
                        SrcUnitFieldName = "FRECEPRICEUNITID",
                        DstUntiFieldName = "FPRICEUNITID",
                        MaterialFieldName = "FMATERIALID",
                        TablePrimaryKey = "FID"
                    };
                    unitConvertParam5.OtherFieldName = new List<string>
                    {
                        "FRECEIVECHARGEQTY",
                        "FORDERID",
                        "FSEQ"
                    };
                    stringBuilder.AppendLine(string.Format(format5, new object[]
                    {
                        this.tmpRptTbl,
                        service.GetUnitConvertSql(unitConvertParam5),
                        num,
                        num2
                    }));
                    lstSql.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
                    stringBuilder.Clear();
                }
            }
        }
        private void MergeOrderBillType(string tempTableName, List<SqlObject> sqlObjList)
        {
            string sql = string.Format("/*dialect*/\r\nMERGE INTO {0} T USING(\r\nSELECT SO.FID,BT.FNAME FBILLTYPENAME\r\n FROM T_SAL_ORDER  SO\r\n LEFT JOIN T_BAS_BILLTYPE_L BT ON SO.FBILLTYPEID=BT.FBILLTYPEID AND BT.FLOCALEID={1}\r\n) BT ON (BT.FID=T.FID)\r\nWHEN MATCHED THEN UPDATE SET T.FBILLTYPE=BT.FBILLTYPENAME {2}", tempTableName, base.Context.UserLocale.LCID, (base.Context.DatabaseType == DatabaseType.MS_SQL_Server) ? ";" : "");
            sqlObjList.Add(new SqlObject(sql, new List<SqlParam>()));
        }
        public string GetSQLWhere()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine(" ");
            stringBuilder.AppendLine(" P.FENTRYID IS NOT NULL");
            stringBuilder.AppendLine(" AND V.FCANCELSTATUS ='A'  AND V.FDOCUMENTSTATUS!='Z' ");
            if (this.filterOrgList.Length > 0)
            {
                stringBuilder.AppendLine(string.Format(" AND V.FSALEORGID IN ({0}) ", this.filterOrgList));
            }
            if (this.filterSoDayFrom > DateTime.MinValue)
            {
                stringBuilder.AppendLine(string.Format(" AND V.FDATE>={0} ", "{ts'" + this.filterSoDayFrom.ToString("yyyy-MM-dd HH:mm:ss") + "'}"));
            }
            if (this.filterSoDayTo < DateTime.MaxValue)
            {
                stringBuilder.AppendLine(string.Format(" AND V.FDATE<{0} ", "{ts'" + this.filterSoDayTo.AddDays(1.0).Date.ToString("yyyy-MM-dd HH:mm:ss") + "'}"));
            }
            if (this.filterCustomerFrom.Trim().Length > 0)
            {
                stringBuilder.AppendLine(string.Format(" AND CUST.FNUMBER>='{0}' ", this.filterCustomerFrom));
            }
            if (this.filterCustomerTo.Trim().Length > 0)
            {
                stringBuilder.AppendLine(string.Format(" AND CUST.FNUMBER<='{0}' ", this.filterCustomerTo));
            }
            if (this.filterSalesFrom.Trim().Length > 0)
            {
                stringBuilder.AppendLine(string.Format(" AND VSM.FNUMBER>='{0}' ", this.filterSalesFrom));
            }
            if (this.filterSalesTo.Trim().Length > 0)
            {
                stringBuilder.AppendLine(string.Format(" AND VSM.FNUMBER<='{0}' ", this.filterSalesTo));
            }
            if (this.filterMaterialFrom.Trim().Length > 0)
            {
                stringBuilder.AppendLine(string.Format(" AND TM.FNUMBER>='{0}' ", this.filterMaterialFrom));
            }
            if (this.filterMaterialTo.Trim().Length > 0)
            {
                stringBuilder.AppendLine(string.Format(" AND TM.FNUMBER<='{0}' ", this.filterMaterialTo));
            }
            if (this.filterSFrom.Trim() != "")
            {
                stringBuilder.AppendLine(string.Format(" AND V.FBILLNO>='{0}' ", this.filterSFrom));
            }
            if (this.filterSTo.Trim() != "")
            {
                stringBuilder.AppendLine(string.Format(" AND V.FBILLNO<='{0}' ", this.filterSTo));
            }
            if (this.filterDeDayFrom > Convert.ToDateTime(DateTime.MinValue.ToShortDateString()))
            {
                stringBuilder.AppendLine(string.Format(" AND PD.FDELIVERYDATE>={0} ", "{ts'" + this.filterDeDayFrom.Date.ToString("yyyy-MM-dd HH:mm:ss") + "'}"));
            }
            if (this.filterDeDayTo < Convert.ToDateTime(DateTime.MaxValue.ToShortDateString()))
            {
                stringBuilder.AppendLine(string.Format(" AND PD.FDELIVERYDATE<{0} ", "{ts'" + this.filterDeDayTo.AddDays(1.0).Date.ToString("yyyy-MM-dd HH:mm:ss") + "'}"));
            }
            if (this.filterFormStatus.Trim() != "ALL")
            {
                stringBuilder.AppendLine(string.Format(" AND V.FDOCUMENTSTATUS='{0}' ", this.filterFormStatus));
            }
            if (this.filterFCStatus.Trim() != "ALL")
            {
                stringBuilder.AppendLine(string.Format(" AND V.FCLOSESTATUS='{0}' ", this.filterFCStatus));
            }
            if (this.filterBCStatus.Trim() != "ALL")
            {
                stringBuilder.AppendLine(string.Format(" AND P.FMRPCLOSESTATUS='{0}' ", this.filterBCStatus));
            }
            if (this.filterMoneyType > 0)
            {
                stringBuilder.AppendLine(string.Format(" AND TF.FSETTLECURRID={0} ", this.filterMoneyType));
            }
            if (this.fEntryid != string.Empty)
            {
                stringBuilder.AppendLine(string.Format(" AND P.fEntryid='{0}' ", this.fEntryid));
            }
            return stringBuilder.ToString();
        }
        protected string GetDataByKey(DynamicObject dy, string key)
        {
            string result = string.Empty;
            if (dy != null && dy[key] != null && !string.IsNullOrWhiteSpace(dy[key].ToString()))
            {
                result = dy[key].ToString();
            }
            return result;
        }
        protected string GetBaseDataByKey(DynamicObject dy, string key, string item)
        {
            string result = string.Empty;
            if (dy != null && dy[key] != null && !string.IsNullOrWhiteSpace(((DynamicObject)dy[key])[item].ToString()))
            {
                result = ((DynamicObject)dy[key])[item].ToString();
            }
            return result;
        }
        private void SetFilter(IRptParams filter)
        {
            DynamicObject parameterData = filter.ParameterData;
            if (parameterData != null)
            {
                this.onTime = Convert.ToBoolean(parameterData["onTime"]);
            }
            DynamicObject customFilter = filter.FilterParameter.CustomFilter;
            int num = filter.CustomParams.Count<KeyValuePair<string, object>>();
            this.filterMoneyType = ((this.GetBaseDataByKey(customFilter, "MoneyType", "Id").Trim() == "") ? 0 : Convert.ToInt32(this.GetBaseDataByKey(customFilter, "MoneyType", "Id")));
            this.filterSoDayFrom = ((this.GetDataByKey(customFilter, "SoFromDate") == string.Empty) ? DateTime.MinValue : Convert.ToDateTime(this.GetDataByKey(customFilter, "SoFromDate")));
            this.filterSoDayTo = ((this.GetDataByKey(customFilter, "SoToDate") == string.Empty) ? DateTime.MaxValue : Convert.ToDateTime(this.GetDataByKey(customFilter, "SoToDate")));
            if (num > 1 && filter.CustomParams.ContainsKey("ParentReportCurrentRow"))
            {
                Dictionary<string, object> dictionary = (Dictionary<string, object>)filter.CustomParams["ParentReportCurrentRow"];
                this.filterSoDayFrom = Convert.ToDateTime(Convert.ToDateTime(dictionary["FDATE"].ToString().Trim()).ToShortDateString());
                this.filterSoDayTo = Convert.ToDateTime(Convert.ToDateTime(dictionary["FDATE"].ToString().Trim()).ToShortDateString());
                this.filterMaterialFrom = dictionary["FMATERIALNUMBER"].ToString().Trim().ToString().Trim();
                this.filterMaterialTo = dictionary["FMATERIALNUMBER"].ToString().Trim().ToString().Trim();
                this.filterSFrom = dictionary["FBILLNO"].ToString().Trim();
                this.filterSTo = dictionary["FBILLNO"].ToString().Trim();
                this.fEntryid = Convert.ToString(dictionary["forderid"]);
            }
            else
            {
                DynamicObject dynamicObject = customFilter["MaterialFrom"] as DynamicObject;
                this.filterMaterialFrom = ((dynamicObject == null) ? "" : this.GetDataByKey(dynamicObject, "NUMBER"));
                DynamicObject dynamicObject2 = customFilter["MaterialTo"] as DynamicObject;
                this.filterMaterialTo = ((dynamicObject2 == null) ? "" : this.GetDataByKey(dynamicObject2, "NUMBER"));
                this.filterSFrom = this.GetDataByKey(customFilter, "SaleOFrom").Trim();
                this.filterSTo = this.GetDataByKey(customFilter, "SaleOTo").Trim();
                this.fEntryid = string.Empty;
            }
            this.priceFrom = "SALORDERBILL";
            object obj;
            //if (num > 1 && filter.CustomParams.ContainsKey("ParentReportFilter"))
            //{
            //    this.bMergingSOHeader = Convert.ToBoolean(((FilterParameter)filter.CustomParams["ParentReportFilter"]).CustomFilter["MergingSOHeader"]);
            //    this.bIncludedUnfilledOrders = Convert.ToBoolean(((FilterParameter)filter.CustomParams["ParentReportFilter"]).CustomFilter["IncludedUnfilledOrders"]);
            //    this.isRecWithMat = Convert.ToBoolean(((FilterParameter)filter.CustomParams["ParentReportFilter"]).CustomFilter["IsRecWithMat"]);
            //    obj = ((FilterParameter)filter.CustomParams["ParentReportFilter"]).CustomFilter["PriceFrom"];
            //}
            //else
            //{
                this.bMergingSOHeader = Convert.ToBoolean(this.GetDataByKey(customFilter, "MergingSOHeader"));
                this.bIncludedUnfilledOrders = Convert.ToBoolean(this.GetDataByKey(customFilter, "IncludedUnfilledOrders"));
                this.isRecWithMat = false;// Convert.ToBoolean(this.GetDataByKey(customFilter, "IsRecWithMat"));
                obj = customFilter["PriceFrom"];
            //}
            this.priceFrom = ((obj == null) ? this.priceFrom : Convert.ToString(obj));
            if (string.IsNullOrWhiteSpace(this.priceFrom))
            {
                this.priceFrom = "SALORDERBILL";
            }
            this.filterOrgList = this.GetDataByKey(customFilter, "SaleOrgList");
            DynamicObject dynamicObject3 = customFilter["CustomerFrom"] as DynamicObject;
            this.filterCustomerFrom = ((dynamicObject3 == null) ? "" : this.GetDataByKey(dynamicObject3, "NUMBER"));
            DynamicObject dynamicObject4 = customFilter["CustomerTo"] as DynamicObject;
            this.filterCustomerTo = ((dynamicObject4 == null) ? "" : this.GetDataByKey(dynamicObject4, "NUMBER"));
            DynamicObject dynamicObject5 = customFilter["SalesFrom"] as DynamicObject;
            this.filterSalesFrom = ((dynamicObject5 == null) ? "" : this.GetDataByKey(dynamicObject5, "NUMBER"));
            DynamicObject dynamicObject6 = customFilter["SalesTo"] as DynamicObject;
            this.filterSalesTo = ((dynamicObject6 == null) ? "" : this.GetDataByKey(dynamicObject6, "NUMBER"));
            this.filterDeDayFrom = ((this.GetDataByKey(customFilter, "DelliveryDateFrom") == string.Empty) ? DateTime.MinValue : Convert.ToDateTime(this.GetDataByKey(customFilter, "DelliveryDateFrom")));
            this.filterDeDayTo = ((this.GetDataByKey(customFilter, "DelliveryDateTo") == string.Empty) ? DateTime.MaxValue : Convert.ToDateTime(this.GetDataByKey(customFilter, "DelliveryDateTo")));
            this.filterFormStatus = this.GetDataByKey(customFilter, "FormStatus");
            this.filterFCStatus = this.GetDataByKey(customFilter, "FormCloseStatus");
            this.filterBCStatus = this.GetDataByKey(customFilter, "BusCloseStatus");
            this.superioreFilter = filter.FilterParameter.FilterString.ToString();
        }
        private string OrgNameList(string idList)
        {
            string text = string.Empty;
            if (idList.Trim().Length > 0)
            {
                IQueryService service = Kingdee.BOS.Contracts.ServiceFactory.GetService<IQueryService>(base.Context);
                QueryBuilderParemeter para = new QueryBuilderParemeter
                {
                    FormId = "ORG_Organizations",
                    SelectItems = SelectorItemInfo.CreateItems("FNAME"),
                    FilterClauseWihtKey = string.Format(" FORGID IN ({0}) AND FLOCALEID={1}", idList, base.Context.UserLocale.LCID.ToString())
                };
                DynamicObjectCollection dynamicObjectCollection = service.GetDynamicObjectCollection(base.Context, para, null);
                foreach (DynamicObject current in dynamicObjectCollection)
                {
                    text = text + Convert.ToString(current["FNAME"]) + ",";
                }
                if (text.Length > 0)
                {
                    text = text.Substring(0, text.Length - 1);
                }
            }
            return text;
        }
        private string IDGet(string id, string idName, string listName, string selectColumn)
        {
            string result = string.Empty;
            if (id.Trim().Length > 0)
            {
                IQueryService service = Kingdee.BOS.Contracts.ServiceFactory.GetService<IQueryService>(base.Context);
                QueryBuilderParemeter para = new QueryBuilderParemeter
                {
                    FormId = listName,
                    SelectItems = SelectorItemInfo.CreateItems(string.Format("{0}", selectColumn)),
                    FilterClauseWihtKey = string.Format(" {0} = '{1}' ", id, idName)
                };
                DynamicObjectCollection dynamicObjectCollection = service.GetDynamicObjectCollection(base.Context, para, null);
                if (dynamicObjectCollection.Count > 0)
                {
                    result = dynamicObjectCollection[0][selectColumn].ToString().Trim();
                }
            }
            return result;
        }


        private void SetHidenColumns(ref IRptParams filter)
        {
            if (this.hidenColumns == null)
            {
                this.hidenColumns = new Dictionary<string, SqlStorageType>();
            }
            else
            {
                this.hidenColumns.Clear();
            }
            this.hidenColumns.Add("FLOCALCURRID", SqlStorageType.SqlBigInt);
            this.hidenColumns.Add("FPRICE_LC", SqlStorageType.SqlDecimal);
           //// this.hidenColumns.Add("FCHARGE_LC", SqlStorageType.SqlDecimal);
            this.hidenColumns.Add("FBASEPRICE_LC", SqlStorageType.SqlDecimal);
            this.hidenColumns.Add("FORDERAMOUNT_LC", SqlStorageType.SqlDecimal);
            //this.hidenColumns.Add("FDELIAMOUNT_LC", SqlStorageType.SqlDecimal);
            this.hidenColumns.Add("FOUTAMOUNT_LC", SqlStorageType.SqlDecimal);
            //this.hidenColumns.Add("FRETURNPAMOUNT_LC", SqlStorageType.SqlDecimal);
            //this.hidenColumns.Add("FRETURNAMOUNT_LC", SqlStorageType.SqlDecimal);
            //this.hidenColumns.Add("FRECAMOUNT_LC", SqlStorageType.SqlDecimal);
            //this.hidenColumns.Add("FWRITEOFFAMOUNT_LC", SqlStorageType.SqlDecimal);
            //this.hidenColumns.Add("FINVOECEAMOUNT_LC", SqlStorageType.SqlDecimal);
            //this.hidenColumns.Add("FADVANCEAMOUNT_LC", SqlStorageType.SqlDecimal);
            //this.hidenColumns.Add("FRECEIPTAMOUNT_LC", SqlStorageType.SqlDecimal);
            //this.hidenColumns.Add("FCHARGEOFFAMOUNT_LC", SqlStorageType.SqlDecimal);
            List<ColumnField> list = new List<ColumnField>();
            foreach (ColumnField current in filter.FilterParameter.ColumnInfo)
            {
                if (this.hidenColumns.ContainsKey(current.FieldName))
                {
                    list.Add(current);
                }
            }
            foreach (ColumnField current2 in list)
            {
                filter.FilterParameter.ColumnInfo.Remove(current2);
            }
        }
        private string GetMergeSQLForARSrc(string tableName)
        {
            string format = "MERGE INTO {0} ST USING  \r\n                        (\r\n\t                        SELECT IT.FIDENTITYID,ROUND(IT.FPRICE,IT.FPRECISION)*IT.RATE*ISNULL(IT.FORDERQTY,0)  AS  FORDERAMOUNT,\r\n                            ROUND(IT.FPRICE,IT.FPRECISION)*IT.RATE*ISNULL(IT.FDELIQTY,0)  AS  FDELIAMOUNT,\r\n                            ROUND(IT.FPRICE,IT.FPRECISION)*IT.RATE*ISNULL(IT.FOUTQTY,0)  AS  FOUTAMOUNT,\r\n                            ROUND(IT.FPRICE,IT.FPRECISION)*IT.RATE*ISNULL(IT.FRETURNPQTY,0)  AS  FRETURNPAMOUNT,\r\n                            ROUND(IT.FPRICE,IT.FPRECISION)*IT.RATE*ISNULL(IT.FRETURNQTY,0)  AS  FRETURNAMOUNT\r\n                            FROM ({1}) IT  \r\n                        )TT ON (ST.FIDENTITYID = TT.FIDENTITYID)\r\n                        WHEN MATCHED THEN UPDATE SET ST.FORDERAMOUNT = TT.FORDERAMOUNT,ST.FDELIAMOUNT = TT.FDELIAMOUNT,\r\n                            ST.FOUTAMOUNT = TT.FOUTAMOUNT, ST.FRETURNPAMOUNT = TT.FRETURNPAMOUNT,ST.FRETURNAMOUNT = TT.FRETURNAMOUNT ";
            IUnitConvertService service = ServiceHelper.GetService<IUnitConvertService>();
            return string.Format(format, tableName, service.GetUnitConvertSql(new UnitConvertParam(true)
            {
                TableName = tableName,
                TableNameAlias = "TMP",
                TablePrimaryKey = "FIDENTITYID",
                MaterialFieldName = "FMATERIALID",
                SrcUnitFieldName = "FRECUNITID",
                DstUntiFieldName = "FUNITID",
                OtherFieldName = new List<string>
                {
                    "FPRICE",
                    "FORDERQTY",
                    "FDELIQTY",
                    "FOUTQTY",
                    "FRETURNPQTY",
                    "FRETURNQTY"
                }
            }));
        }





        #region 原来代码注释掉不用
        //public static void CreateInvoceBillno_RTable(string invoceBillno_RTable, List<SqlObject> lstTable)
        //{
        //    StringBuilder stringBuilder = new StringBuilder();
        //    stringBuilder.AppendLine(string.Format(" CREATE TABLE {0}", invoceBillno_RTable));
        //    stringBuilder.AppendLine("(");
        //    stringBuilder.AppendLine("  FSRCROWID INT NULL ");
        //    stringBuilder.AppendLine("  ,FINVOCEBILLNO_R  nvarchar(1000) NULL ");
        //    stringBuilder.AppendLine(" )");
        //    lstTable.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
        //    stringBuilder.Clear();
        //}
        //public void CreateRecSeqTable(string tmpTable, List<SqlObject> lstTable)
        //{
        //    StringBuilder stringBuilder = new StringBuilder();
        //    stringBuilder.AppendLine(string.Format(" CREATE TABLE {0} ", this.recSeqTable));
        //    stringBuilder.AppendLine(" (");
        //    stringBuilder.AppendLine("     FRECBILLID INT  null, ");
        //    stringBuilder.AppendLine("     FSEQ INT  null  ");
        //    stringBuilder.AppendLine(" ) ");
        //    lstTable.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
        //    stringBuilder.Clear();
        //}
        //public void CreateRecAmountTable(string tmpTable, List<SqlObject> lstTable)
        //{
        //    StringBuilder stringBuilder = new StringBuilder();
        //    stringBuilder.AppendLine(string.Format(" CREATE TABLE {0} ", this.recAmountTable));
        //    stringBuilder.AppendLine(" (");
        //    stringBuilder.AppendLine("     FRECEIPTAMOUNT  decimal(24,10)  null , ");
        //    stringBuilder.AppendLine("     FCHARGEOFFAMOUNT  decimal(24,10)  null , ");
        //    stringBuilder.AppendLine("     FRECBILLID INT  null, ");
        //    stringBuilder.AppendLine("     FSEQ INT  null  ");
        //    stringBuilder.AppendLine(" ) ");
        //    lstTable.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
        //    stringBuilder.Clear();
        //}

        #endregion


    }
}
