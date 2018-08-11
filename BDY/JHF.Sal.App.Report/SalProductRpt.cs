using Kingdee.BOS;
using Kingdee.BOS.App;
using Kingdee.BOS.App.Core.BusinessFlow;
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
    public class SalProductRpt : SysReportBaseService
    {

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
        public int filterShipment;
        public int filterOutStock;
        public int filterInvoece;
        public int filterReceipt;
        public int filterReturnS;
        public SalRptEnums.Enu_Statistics filterSTotal = SalRptEnums.Enu_Statistics.SOId;
        public SalRptEnums.Enu_Statistics filterTotal = SalRptEnums.Enu_Statistics.CustomerId;
        public decimal filterStotalo;
        public decimal filtertotalo;
        public string superioreFilter = string.Empty;
        protected bool onTime = true;
        private string rptFilterTable = string.Empty;
        private string priceFrom = string.Empty;
        private bool isRecWithMat;
        private Dictionary<string, SqlStorageType> hidenColumns;
        protected StringBuilder creatWhereFirst = new StringBuilder();
        protected StringBuilder creatWhereSecond = new StringBuilder();
        protected StringBuilder creatWhereThrid = new StringBuilder();
        protected string filterTable = string.Empty;
        protected string moreFilter = string.Empty;
        protected string noCalTable = string.Empty;
        protected string tempTableName = string.Empty;
        protected string orderBy = string.Empty;
        private List<string> sqlList = new List<string>();
        private string sourceTable = string.Empty;
        private string flowDataTable = string.Empty;
        private List<string> deleteTables = new List<string>();
        private string flowTable = string.Empty;
        private string filterSql = string.Empty;
        private string receivableHelpTable = string.Empty;
        private string receivableTable = string.Empty;
        private Dictionary<string, string> auxPropValues;
        private IViewService iserver;
        private FormMetadata flexMetadata;
        private Dictionary<long, DynamicObject> auxPropDatas = new Dictionary<long, DynamicObject>();
        private List<DecimalControlField> lstDcf = new List<DecimalControlField>();
        public override void Initialize()
        {
            base.Initialize();
            base.ReportProperty.ReportType = ReportType.REPORTTYPE_NORMAL;
            base.ReportProperty.ReportName = new LocaleValue("销售订单执行汇总表");
            base.ReportProperty.DetailReportId = "SAL_DetailReport";
            base.ReportProperty.PrimaryKeyFieldName = "FBILLNO";
            base.ReportProperty.IsGroupSummary = true;
            base.ReportProperty.IsUIDesignerColumns = true;
            base.ReportProperty.DspInsteadColumnsInfo.DefaultDspInsteadColumns.Add("FSALEORGID", "FSALEORGNAME");
            base.ReportProperty.DspInsteadColumnsInfo.DefaultDspInsteadColumns.Add("FSALEDEPTID", "FSALEDEPT");
            base.ReportProperty.DspInsteadColumnsInfo.DefaultDspInsteadColumns.Add("FSALEGROUPID", "FSALEGROUP");
            base.ReportProperty.DspInsteadColumnsInfo.DefaultDspInsteadColumns.Add("FSALERID", "FSALES");
            base.ReportProperty.DspInsteadColumnsInfo.DefaultDspInsteadColumns.Add("FMATERIALID", "FMATERIALNUMBER");
            base.ReportProperty.DspInsteadColumnsInfo.DefaultDspInsteadColumns.Add("FMAPID", "FMAPNUMBER");
            base.ReportProperty.DspInsteadColumnsInfo.DefaultDspInsteadColumns.Add("FCURRENCYID", "FMONEYTYPE");
            base.ReportProperty.DspInsteadColumnsInfo.DefaultDspInsteadColumns.Add("FCUSTID", "FCUSTOMERNUMBER");
            base.ReportProperty.DspInsteadColumnsInfo.DefaultDspInsteadColumns.Add("FMATERIALGROUP", "FMATERIALGROUPNAME");
            base.ReportProperty.DspInsteadColumnsInfo.DefaultDspInsteadColumns.Add("FAUXPROPID", "FAUXPROP");
            base.ReportProperty.DspInsteadColumnsInfo.DefaultDspInsteadColumns.Add("FUNITID", "FUNITNAME");
            base.ReportProperty.DspInsteadColumnsInfo.DefaultDspInsteadColumns.Add("FPRICEUNITID", "FPRICEUNITNAME");
            this.SetDecimalControl();
        }
        public override ReportTitles GetReportTitles(IRptParams filter)
        {
            DynamicObject customFilter = filter.FilterParameter.CustomFilter;
            if (customFilter["SaleOrgList"] == null || string.IsNullOrWhiteSpace(customFilter["SaleOrgList"].ToString()))
            {
                return base.GetReportTitles(filter);
            }
            return this.BuilderTitle(filter);
        }
        public override void BuilderReportSqlAndTempTable(IRptParams filter, string tableName)
        {
            this.SetFilter(filter);
            this.ModifyShowHideColumns(ref filter);
            this.InitTables();
            this.SetHidenColumns(ref filter);
            this.moreFilter = filter.FilterParameter.FilterString.Trim();
            DynamicObject parameterData = filter.ParameterData;
            if (parameterData != null)
            {
                this.onTime = Convert.ToBoolean(parameterData["onTime"]);
            }
            DynamicObject customFilter = filter.FilterParameter.CustomFilter;
            if (customFilter["SaleOrgList"] == null || string.IsNullOrWhiteSpace(customFilter["SaleOrgList"].ToString()))
            {
                this.SetRptBosTable(tableName);
                return;
            }
            this.GetSQLWhere();
            List<BaseDataTempTable> baseDataTempTable = filter.BaseDataTempTable;
            if (!this.filterOrgList.IsNullOrEmptyOrWhiteSpace())
            {
                baseDataTempTable = SalRptCommon.GetBaseDataTempTable(base.Context, this.filterOrgList, "SAL_CollectReport");
            }
            SalRptCommon.GetFilterOrder(base.Context, this.rptFilterTable, !string.IsNullOrWhiteSpace(this.moreFilter), this.creatWhereFirst.ToString(), this.filterOrgList, baseDataTempTable, "SAL_CollectReport");
            foreach (BaseDataTempTable current in baseDataTempTable)
            {
                this.deleteTables.Add(current.TempTable);
            }
            this.GetDataSource(this.rptFilterTable);
            this.CreatSQLWhereForTemp();
            this.GetFinalSummaryData();
            this.SetRptBosTable(tableName);
        }
        public override void CloseReport()
        {
            if (this.deleteTables.Count > 0)
            {
                SalRptCommon.DropTempTables(base.Context, this.deleteTables);
            }
        }
        public override List<Kingdee.BOS.Core.Report.SummaryField> GetSummaryColumnInfo(IRptParams filter)
        {
            return new List<Kingdee.BOS.Core.Report.SummaryField>
            {
                new Kingdee.BOS.Core.Report.SummaryField("FSOAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FSAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FALREADYAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FNOTAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FRETURNPAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FRETURNSAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FNVOECEAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FNNVOECEAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FADVANCEAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FRECEIPTAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FNRECEIPTAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FCHARGEOFFAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FSOQTY", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FORDERCHARGEQTY", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FSQTY", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FDELICHARGEQTY", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FALREADYQTY", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FHAVEOUTCHARGEQTY", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FNOTQTY", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FNOTOUTCHARGEQTY", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FRETURNPQTY", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FRETURNPCHARGEQTY", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FRETURNSQTY", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FRETURNCHARGEQTY", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FINVOECEQTY", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FINVOECECHARGEQTY", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FNINVOECEQTY", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FNINVOECECHARGEQTY", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FRECQTY", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FRECECHARGEQTY", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FRECAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FRECNQTY", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FRECNCHARGEQTY", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FRECNAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FWRITEOFFAMOUNT", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FOrderBaseQty", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FDeliveryNoticeQty", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FDeliveryQty", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FUnDeliveryQty", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FReturnNoticeQty", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FReturnBaseQty", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FReceiveQty", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FUnConfirmQty", BOSEnums.Enu_SummaryType.SUM),
                new Kingdee.BOS.Core.Report.SummaryField("FBaseInvoiceQty", BOSEnums.Enu_SummaryType.SUM)
            };
        }
        public void GetDataSource(string rptFilterTable)
        {
            string entityKey = "FSaleOrderEntry";
            long[] entryIds = SalRptCommon.GetEntryIds(base.Context, rptFilterTable, this.moreFilter, out this.filterSql);
            List<string> list = new List<string>();
            if (entryIds.Length > 0)
            {
                BusinessFlowDataService businessFlowDataService = new BusinessFlowDataService();
                BuildBFTrackerTempTableArgs buildBFTrackerTempTableArgs = new BuildBFTrackerTempTableArgs("SAL_SaleOrder", entityKey, entryIds);
                buildBFTrackerTempTableArgs.OnTime = this.onTime;
                buildBFTrackerTempTableArgs.IsInt64 = true;
                string text = this.ReadEntryTableName("SAL_SaleOrder", "FSaleOrderEntry");
                buildBFTrackerTempTableArgs.FirstTableName = text;
                buildBFTrackerTempTableArgs.TableNames.Add(text);
                buildBFTrackerTempTableArgs.TableNames.Add(text);
                buildBFTrackerTempTableArgs.TableNames.Add(this.ReadEntryTableName("SAL_DELIVERYNOTICE", "FEntity"));
                buildBFTrackerTempTableArgs.TableNames.Add(this.ReadEntryTableName("SAL_OUTSTOCK", "FEntity"));
                buildBFTrackerTempTableArgs.TableNames.Add(this.ReadEntryTableName("SAL_RETURNNOTICE", "FEntity"));
                buildBFTrackerTempTableArgs.TableNames.Add(this.ReadEntryTableName("SAL_RETURNSTOCK", "FEntity"));
                buildBFTrackerTempTableArgs.TableNames.Add(this.ReadEntryTableName("AR_receivable", "FEntityDetail"));
                buildBFTrackerTempTableArgs.TableNames.Add(this.ReadEntryTableName("SAL_INITOUTSTOCK", "FEntity"));
                buildBFTrackerTempTableArgs.TableNames.Add(this.ReadEntryTableName("PLN_PLANORDER", "FEntity"));

                
                BuildBFTrackerTempTableResult buildBFTrackerTempTableResult = businessFlowDataService.BuildBFTrackerTempTable(base.Context, buildBFTrackerTempTableArgs);
                if (buildBFTrackerTempTableResult != null)
                {
                    string text2 = buildBFTrackerTempTableResult.TempTableName;
                    this.deleteTables.Add(text2);
                    foreach (string current in buildBFTrackerTempTableResult.TableColumns)
                    {
                        if (buildBFTrackerTempTableArgs.TableNames.Contains(current) && !list.Contains(current))
                        {
                            list.Add(current);
                        }
                    }
                    SalRptCommon.GetNeedDataByFields(base.Context, list, text2, this.flowDataTable, entryIds);
                    this.deleteTables.Add(this.flowDataTable);
                    SalRptCommon.InsertFlowData(base.Context, this.flowTable, list, this.flowDataTable, this.filterSql, entryIds, this.bIncludedUnfilledOrders);
                    SalRptCommon.InsertReceivableData(base.Context, this.receivableHelpTable, this.receivableTable, this.flowTable);
                    return;
                }
                SalRptCommon.InsertCustomFlowData(base.Context, this.flowTable, rptFilterTable, this.moreFilter);
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
        public void CreateTmpRptTable(string rptTableName, List<SqlObject> lstTable)
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine(string.Format(" CREATE TABLE {0} (  ", rptTableName));
            stringBuilder.AppendLine(" FPRECISION INT,");
            stringBuilder.AppendLine(" FPRICEDIGITS INT,");
            stringBuilder.AppendLine(" FAMOUNTDIGITS INT,");
            stringBuilder.AppendLine("     FSALEORGID int null,  ");
            stringBuilder.AppendLine("     FSALEORGNUMBER NVARCHAR(100),");
            stringBuilder.AppendLine("     FSALEORGNAME nvarchar(100) null ,");
            stringBuilder.AppendLine("     FID INT,");
            stringBuilder.AppendLine("     FORDERID INT,");
            stringBuilder.AppendLine("     FRECID INT,");
            stringBuilder.AppendLine("     FBILLNO varchar(80) null ,  ");
            stringBuilder.AppendLine("     FBILLTYPE nvarchar(100) null ,  ");
            stringBuilder.AppendLine("     FDATE DateTime ,  ");
            stringBuilder.AppendLine("     FSALEDEPTID int null,  ");
            stringBuilder.AppendLine("     FSALEDEPT  nvarchar(255) null,  ");
            stringBuilder.AppendLine("     FSALEGROUPID int null,  ");
            stringBuilder.AppendLine("     FSALEGROUP  nvarchar(255) null, ");
            stringBuilder.AppendLine("     FSALERID int null,  ");
            stringBuilder.AppendLine("     FSALERNUMBER nvarchar(100) null,  ");
            stringBuilder.AppendLine("     FSALES  nvarchar(255) null, ");
            stringBuilder.AppendLine("     FCUSTID INT NULL, ");
            stringBuilder.AppendLine("     FCUSTOMERNUMBER nvarchar(100) null,  ");
            stringBuilder.AppendLine("     FCUSTOMERNAME nvarchar(255)  null, ");
            stringBuilder.AppendLine("     FDELIVERYDAY DateTime , ");
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
            stringBuilder.AppendLine("     FUNITID  nvarchar(100) null, ");
            stringBuilder.AppendLine("     FUNITNAME  nvarchar(100) null, ");
            stringBuilder.AppendLine("     FPRICEUNITID  nvarchar(100) null, ");
            stringBuilder.AppendLine("     FPRICEUNITNAME  nvarchar(100) null, ");
            stringBuilder.AppendLine("     FRECUNITID INT NULL,");
            stringBuilder.AppendLine("     FCURRENCYID INT NULL,  ");
            stringBuilder.AppendLine("     FMONEYTYPE nvarchar(100) null,");
            stringBuilder.AppendLine("     FPRICE  decimal(24,10)  null , ");
            stringBuilder.AppendLine("     FCHARGE  decimal(24,10)  null , ");
            stringBuilder.AppendLine("     FCLOSESTATUS  nvarchar(10)  null , ");
            stringBuilder.AppendLine("     FMRPCLOSESTATUS  nvarchar(10)  null , ");
            stringBuilder.AppendLine("     FMRPTERMINATESTATUS  nvarchar(10)  null , ");
            stringBuilder.AppendLine(" FSOQTY DECIMAL(24,10),");
            stringBuilder.AppendLine(" FORDERCHARGEQTY DECIMAL(24,10),");
            stringBuilder.AppendLine(" FSOAMOUNT DECIMAL(24,10),");
            stringBuilder.AppendLine(" FSQTY DECIMAL(24,10),");
            stringBuilder.AppendLine(" FDELIPRICEUNITID int, ");
            stringBuilder.AppendLine(" FDELICHARGEQTY DECIMAL(24,10),");
            stringBuilder.AppendLine(" FSAMOUNT DECIMAL(24,10),");
            stringBuilder.AppendLine(" FALREADYQTY DECIMAL(24,10),");
            stringBuilder.AppendLine(" FOUTPRICEUNITID int,  ");
            stringBuilder.AppendLine(" FHAVEOUTCHARGEQTY DECIMAL(24,10),");
            stringBuilder.AppendLine(" FALREADYAMOUNT DECIMAL(24,10),");
            stringBuilder.AppendLine(" FNOTQTY DECIMAL(24,10),");
            stringBuilder.AppendLine(" FNOTOUTCHARGEQTY DECIMAL(24,10),");
            stringBuilder.AppendLine(" FNOTAMOUNT DECIMAL(24,10),");
            stringBuilder.AppendLine(" FRETURNPQTY DECIMAL(24,10),");
            stringBuilder.AppendLine(" FRETNOTICEPRICEUNITID int, ");
            stringBuilder.AppendLine(" FRETURNPCHARGEQTY DECIMAL(24,10),");
            stringBuilder.AppendLine(" FRETURNPAMOUNT DECIMAL(24,10),");
            stringBuilder.AppendLine(" FRETURNSQTY DECIMAL(24,10),");
            stringBuilder.AppendLine(" FRETURNPRICEUNITID int, ");
            stringBuilder.AppendLine(" FRETURNCHARGEQTY DECIMAL(24,10),");
            stringBuilder.AppendLine(" FRETURNSAMOUNT DECIMAL(24,10),");
            stringBuilder.AppendLine(" FRECQTY DECIMAL(24,10),");
            stringBuilder.AppendLine(" FRECEPRICEUNITID int, ");
            stringBuilder.AppendLine(" FRECECHARGEQTY DECIMAL(24,10),");
            stringBuilder.AppendLine(" FRECAMOUNT DECIMAL(24,10),");
            stringBuilder.AppendLine(" FRECNQTY DECIMAL(24,10),");
            stringBuilder.AppendLine(" FRECNCHARGEQTY DECIMAL(24,10),");
            stringBuilder.AppendLine(" FRECNAMOUNT DECIMAL(24,10),");
            stringBuilder.AppendLine(" FWRITEOFFAMOUNT DECIMAL(24,10),");
            stringBuilder.AppendLine(" FSETACCOUNTTYPE  nvarchar(100)  null , ");
            stringBuilder.AppendLine(" FINVOECEQTY DECIMAL(24,10),");
            stringBuilder.AppendLine(" FINVOECECHARGEQTY DECIMAL(24,10),");
            stringBuilder.AppendLine(" FNVOECEAMOUNT DECIMAL(24,10),");
            stringBuilder.AppendLine(" FNINVOECEQTY DECIMAL(24,10),");
            stringBuilder.AppendLine(" FNINVOECECHARGEQTY DECIMAL(24,10),");
            stringBuilder.AppendLine(" FNNVOECEAMOUNT DECIMAL(24,10),");
            stringBuilder.AppendLine(" FRECEIPTAMOUNT DECIMAL(24,10),");
            stringBuilder.AppendLine(" FNRECEIPTAMOUNT DECIMAL(24,10),");
            stringBuilder.AppendLine(" FCHARGEOFFAMOUNT DECIMAL(24,10),");
            stringBuilder.AppendLine(" FADVANCEAMOUNT DECIMAL(24,10),");
            stringBuilder.AppendLine("     FBASEUNITID_ID  int  not null default (0), ");
            stringBuilder.AppendLine("     FBASEUNITID nvarchar(100) null,");
            stringBuilder.AppendLine("     FORDERBASEQTY decimal(24,10) null default(0),");
            stringBuilder.AppendLine("     FBASEPRICE decimal(24,10) null default(0),");
            stringBuilder.AppendLine("     FDELIVERYNOTICEQTY decimal(24,10) null default(0),");
            stringBuilder.AppendLine("     FDELIVERYQTY decimal(24,10) null default(0),");
            stringBuilder.AppendLine("     FUNDELIVERYQTY decimal(24,10) null default(0),");
            stringBuilder.AppendLine("     FRETURNNOTICEQTY decimal(24,10) null default(0),");
            stringBuilder.AppendLine("     FRETURNBASEQTY decimal(24,10) null default(0),");
            stringBuilder.AppendLine("     FRECEIVEQTY decimal(24,10) null default(0),");
            stringBuilder.AppendLine("     FUNCONFIRMQTY decimal(24,10) null default(0),");
            stringBuilder.AppendLine("     FBASEINVOICEQTY decimal(24,10) null default(0),");
            stringBuilder.AppendLine("     FISRECMAT  char(1)  null,");
            stringBuilder.AppendLine(" FROWNO INT  )   ");
            lstTable.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
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
        private void CreateDetailTable(string noCalTable, List<SqlObject> lstTable)
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine(string.Format(" CREATE TABLE {0} ( ", noCalTable));
            stringBuilder.AppendLine(" FROW INT,");
            stringBuilder.AppendLine(" FORDERQTY DECIMAL(23,10) , ");
            stringBuilder.AppendLine(" FORDERPRICEUNITID int , ");
            stringBuilder.AppendLine(" FORDERCHARGEQTY DECIMAL(23,10) , ");
            stringBuilder.AppendLine(" FORDERID INT, ");
            stringBuilder.AppendLine(" FRECEIVERID INT, ");
            stringBuilder.AppendLine(" FSOFID INT, ");
            stringBuilder.AppendLine(" FRECID INT, ");
            stringBuilder.AppendLine(" FDELIVERQTY DECIMAL(23,10), ");
            stringBuilder.AppendLine(" FDELIPRICEUNITID int, ");
            stringBuilder.AppendLine(" FDELICHARGEQTY DECIMAL(23,10), ");
            stringBuilder.AppendLine(" FOUTQTY DECIMAL(23,10),  ");
            stringBuilder.AppendLine(" FOUTPRICEUNITID int,  ");
            stringBuilder.AppendLine(" FOUTCHARGEQTY DECIMAL(23,10),  ");
            stringBuilder.AppendLine(" FREMAINOUTQTY DECIMAL(23,10),  ");
            stringBuilder.AppendLine(" FNOTOUTCHARGEQTY DECIMAL(23,10),  ");
            stringBuilder.AppendLine(" FRETNOTICEQTY DECIMAL(23,10),");
            stringBuilder.AppendLine(" FRETNOTICEPRICEUNITID int, ");
            stringBuilder.AppendLine(" FRETNOTICECHARGEQTY DECIMAL(23,10), ");
            stringBuilder.AppendLine(" FRETURNQTY DECIMAL(23,10), ");
            stringBuilder.AppendLine(" FRETURNPRICEUNITID int, ");
            stringBuilder.AppendLine(" FRETURNCHARGEQTY DECIMAL(23,10), ");
            stringBuilder.AppendLine(" FARQTY  DECIMAL(23,10),");
            stringBuilder.AppendLine(" FARPRICEUNITID int, ");
            stringBuilder.AppendLine(" FARCHARGEQTY DECIMAL(23,10), ");
            stringBuilder.AppendLine(" FARAMOUNT DECIMAL(23,10), ");
            stringBuilder.AppendLine(" FWRITEOFFAMOUNT DECIMAL(23,10), ");
            stringBuilder.AppendLine(" FSETACCOUNTTYPE  nvarchar(100)  null , ");
            stringBuilder.AppendLine(" FIAMOUNT DECIMAL(23,10), ");
            stringBuilder.AppendLine(" FIQTY DECIMAL(23,10),");
            stringBuilder.AppendLine(" FIPRICEUNITID int, ");
            stringBuilder.AppendLine(" FICHARGEQTY DECIMAL(23,10), ");
            stringBuilder.AppendLine(" FNIAMOUNT DECIMAL(23,10), ");
            stringBuilder.AppendLine(" FNIQTY DECIMAL(23,10),");
            stringBuilder.AppendLine(" FNIPRICEUNITID int, ");
            stringBuilder.AppendLine(" FNICHARGEQTY DECIMAL(23,10), ");
            stringBuilder.AppendLine(" FICAMOUNT DECIMAL(23,10),");
            stringBuilder.AppendLine(" FICQTY DECIMAL(23,10),");
            stringBuilder.AppendLine(" FNICAMOUNT DECIMAL(23,10),");
            stringBuilder.AppendLine(" FNICQTY DECIMAL(23,10),");
            stringBuilder.AppendLine(" FCHARGEOFFAMOUNT DECIMAL(23,10),");
            stringBuilder.AppendLine("     FBASEUNITID_ID  int, ");
            stringBuilder.AppendLine("     FBASEUNITID nvarchar(100),");
            stringBuilder.AppendLine("     FORDERBASEQTY decimal(24,10),");
            stringBuilder.AppendLine("     FBASEPRICE decimal(24,10),");
            stringBuilder.AppendLine("     FDELIVERYNOTICEQTY decimal(24,10),");
            stringBuilder.AppendLine("     FDELIVERYQTY decimal(24,10),");
            stringBuilder.AppendLine("     FUNDELIVERYQTY decimal(24,10),");
            stringBuilder.AppendLine("     FRETURNNOTICEQTY decimal(24,10),");
            stringBuilder.AppendLine("     FRETURNBASEQTY decimal(24,10),");
            stringBuilder.AppendLine("     FRECEIVEQTY decimal(24,10),");
            stringBuilder.AppendLine("     FUNCONFIRMQTY decimal(24,10),");
            stringBuilder.AppendLine("     FBASEINVOICEQTY decimal(24,10)");
            stringBuilder.AppendLine(" )");
            lstTable.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
        }
        public void GetFinalSummaryData()
        {
            List<SqlObject> list = new List<SqlObject>();
            this.sqlList.Clear();
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine(string.Format(" INSERT /*+append*/ INTO {0} ", this.noCalTable));
            stringBuilder.AppendLine(" ( ");
            stringBuilder.AppendLine(" FSOFID,FORDERID,FRECEIVERID,FORDERQTY,FORDERPRICEUNITID,FORDERCHARGEQTY,");
            stringBuilder.AppendLine(" FDELIVERQTY,FDELIPRICEUNITID,FDELICHARGEQTY,");
            stringBuilder.AppendLine(" FOUTQTY,FOUTPRICEUNITID,FOUTCHARGEQTY,FREMAINOUTQTY,FNOTOUTCHARGEQTY,");
            stringBuilder.AppendLine(" FRETNOTICEQTY,FRETNOTICEPRICEUNITID, FRETNOTICECHARGEQTY, ");
            stringBuilder.AppendLine(" FRETURNQTY,FRETURNPRICEUNITID, FRETURNCHARGEQTY, ");
            stringBuilder.AppendLine(" FARQTY,FARPRICEUNITID,FARCHARGEQTY,FARAMOUNT,FWRITEOFFAMOUNT,FSETACCOUNTTYPE,");
            stringBuilder.AppendLine(" FIAMOUNT,FIQTY,FICHARGEQTY,");
            stringBuilder.AppendLine(" FICAMOUNT,FNICAMOUNT,FCHARGEOFFAMOUNT,FRECID,");
            stringBuilder.AppendLine(" FBASEUNITID_ID,FORDERBASEQTY,FBASEPRICE,FDELIVERYNOTICEQTY,FDELIVERYQTY,FUNDELIVERYQTY,FRETURNNOTICEQTY,FRETURNBASEQTY,FRECEIVEQTY,FUNCONFIRMQTY,FBASEINVOICEQTY,");
            stringBuilder.AppendLine(" FROW");
            stringBuilder.AppendLine(" )");
            stringBuilder.AppendLine(" SELECT DISTINCT T8.FSOFId AS FSOFID , PD.FENTRYID AS FORDERID, TARE.FENTRYID AS FRECEIVERID,");
            stringBuilder.AppendLine(" PD.FBASEUNITQTY AS FORDERQTY ,TSOEF.FPRICEUNITID AS FORDERPRICEUNITID,TSOEF.FPRICEUNITQTY AS FORDERCHARGEQTY,");
            stringBuilder.AppendLine(" PR.FBASEDELIQTY AS FDELIVERQTY,TSDEF.FPRICEUNITID AS FDELIPRICEUNITID,TSDEF.FPRICEUNITQTY AS FDELICHARGEQTY,");
            stringBuilder.AppendLine(" PR.FBASESTOCKOUTQTY AS FOUTQTY,TSOSEF.FPRICEUNITID AS FOUTPRICEUNITID,TSOSEF.FPRICEUNITQTY AS FOUTCHARGEQTY,");
            stringBuilder.AppendLine(" PR.FBASEREMAINOUTQTY AS FREMAINOUTQTY, 0 AS FNOTOUTCHARGEQTY, ");
            stringBuilder.AppendLine(" PR.FBASERETNOTICEQTY AS FRETNOTICEQTY,TSRNEF.FPRICEUNITID AS FRETNOTICEPRICEUNITID,TSRNEF.FPRICEUNITQTY AS FRETNOTICECHARGEQTY, ");
            stringBuilder.AppendLine(" PR.FBASERETURNQTY AS FRETURNQTY,TSRSEF.FPRICEUNITID AS FRETURNPRICEUNITID,TSRSEF.FPRICEUNITQTY AS FRETURNCHARGEQTY,");
            stringBuilder.AppendLine(" PR.FBASEARQTY AS FARQTY, TARE.FPRICEUNITID AS FARPRICEUNITID, ");
            stringBuilder.AppendLine(" ( CASE WHEN ISNULL(FARQTY,0)=0  THEN NULL ELSE TARE.FPRICEQTY END) AS FARCHARGEQTY,");
            stringBuilder.AppendLine(" T7.FARAMOUNT AS FARAMOUNT, T7.FWRITEOFFAMOUNT AS FWRITEOFFAMOUNT, T7.FSETACCOUNTTYPE AS FSETACCOUNTTYPE,T7.FIAMOUNT AS FIAMOUNT, T7.FIQTY AS FIQTY, T7.FICHARGEQTY AS FICHARGEQTY,");
            stringBuilder.AppendLine(" T8.FICAMOUNT AS FICAMOUNT, T8.FNICAMOUNT AS FNICAMOUNT,T8.FCHARGEOFFAMOUNT AS FCHARGEOFFAMOUNT,TMP.FRECID AS FRECID, ");
            stringBuilder.AppendLine(" PD.FBASEUNITID,PD.FBASEUNITQTY, ");
            stringBuilder.AppendLine(" (CASE WHEN ISNULL(PD.FBASEUNITQTY,0)=0 THEN 0  ELSE  ISNULL(TSOEF.FAMOUNT,0)/PD.FBASEUNITQTY END) AS FBASEPRICE, ");
            stringBuilder.AppendLine(" TSDE.FBASEUNITQTY, ");
            stringBuilder.AppendLine(" TSOSE.FBASEUNITQTY, ");
            stringBuilder.AppendLine(" PD.FBASEUNITQTY-TSOSE.FBASEUNITQTY, ");
            stringBuilder.AppendLine(" TSRE.FBASEUNITQTY, ");
            stringBuilder.AppendLine(" TSRSE.FBASEUNITQTY, ");
            stringBuilder.AppendLine(" TARE.FBASICUNITQTY, ");
            stringBuilder.AppendLine(" PD.FBASEUNITQTY-TARE.FBASICUNITQTY, ");
            stringBuilder.AppendLine(" TARE.FBUYIVBASICQTY, ");
            stringBuilder.AppendLine(" ROW_NUMBER() OVER (ORDER BY PD.FENTRYID ASC )  AS FROW");
            stringBuilder.AppendLine(string.Format(" FROM {0} AS TMP", this.flowTable));
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_SAL_ORDERENTRY PD ON TMP.FORDERID = PD.FENTRYID");
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_SAL_ORDERENTRY_R PR ON PR.FENTRYID = PD.FENTRYID");
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_SAL_ORDERENTRY_F TSOEF ON PR.FENTRYID = TSOEF.FENTRYID");
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_SAL_DELIVERYNOTICEENTRY TSDE ON TMP.FDENOID=TSDE.FENTRYID");
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_SAL_DELIVERYNOTICEENTRY_F TSDEF ON TMP.FDENOID = TSDEF.FENTRYID");
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_SAL_OUTSTOCKENTRY TSOSE  ON TMP.FOUTID=TSOSE.FENTRYID AND TMP.FOUTTYPE ='1'");
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_SAL_OUTSTOCKENTRY_F TSOSEF ON TMP.FOUTID = TSOSEF.FENTRYID");
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_SAL_RETURNNOTICEENTRY TSRE  ON TMP.FRETNOID=TSRE.FENTRYID ");
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_SAL_RETURNNOTICEENTRY_F TSRNEF ON TMP.FRETNOID = TSRNEF.FENTRYID");
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_SAL_RETURNSTOCKENTRY TSRSE ON TMP.FRETURNID=TSRSE.FENTRYID ");
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_SAL_RETURNSTOCKENTRY_F TSRSEF ON TMP.FRETURNID = TSRSEF.FENTRYID");
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_AR_RECEIVABLEENTRY TARE ON TMP.FRECID = TARE.FENTRYID ");
            stringBuilder.AppendLine(" LEFT OUTER JOIN ");
            stringBuilder.AppendLine(" (SELECT TSO.FORDERID,");
            stringBuilder.AppendLine(" (CASE WHEN TAR.FSETACCOUNTTYPE='1' THEN SUM(ISNULL(TSO.FBASERECQTY,0)/(case ISNULL(TARO.FSALBASEQTY,1) when  0 then 1 else ISNULL(TARO.FSALBASEQTY,1) end)*ISNULL(TARS.FIVALLAMOUNTFOR,0))\r\n                                 WHEN TAR.FSETACCOUNTTYPE='3' THEN SUM(ISNULL(TABLETIV.FALLAMOUNTFOR,0)) ELSE 0 END) AS FIAMOUNT,");
            stringBuilder.AppendLine(" (CASE WHEN TAR.FSETACCOUNTTYPE='1' THEN SUM(ISNULL(TSO.FBASERECQTY,0)/(case ISNULL(TARO.FSALBASEQTY,1) when  0 then 1 else ISNULL(TARO.FSALBASEQTY,1) end)*ISNULL(TSO.FOPENQTY,0)) \r\n                                WHEN TAR.FSETACCOUNTTYPE='3' THEN SUM(CASE WHEN ISNULL(TARS.FPRICEQTY,0)=0 THEN 0 ELSE ISNULL(TARO.FSALQTY,0)*ISNULL(TABLETIV.FPRICEQTY,0)/TARS.FPRICEQTY END) ELSE 0 END) AS FIQTY,");
            stringBuilder.AppendLine(" (CASE WHEN TAR.FSETACCOUNTTYPE='1' THEN SUM(ISNULL(TSO.FRECPRICEQTY,0)/(case ISNULL(TARO.FSALBASEQTY,1) when  0 then 1 else ISNULL(TARO.FSALBASEQTY,1) end)*ISNULL(TSO.FOPENQTY,0))\r\n                                WHEN TAR.FSETACCOUNTTYPE='3' THEN SUM(ISNULL(TABLETIV.FPRICEQTY,0)) ELSE 0 END) AS FICHARGEQTY, ");
            stringBuilder.AppendLine(" SUM(ISNULL(TSO.FBASERECQTY,0)/(case ISNULL(TARO.FSALBASEQTY,1) when  0 then 1 else ISNULL(TARO.FSALBASEQTY,1) end)*ISNULL(TARS.FALLAMOUNTFOR,0)) AS FARAMOUNT,");
            stringBuilder.AppendLine(" SUM(TARB.FCUROPENAMOUNTFOR) AS FWRITEOFFAMOUNT, ");
            stringBuilder.AppendLine(string.Format(" CASE WHEN TAR.FSETACCOUNTTYPE='1' THEN N'{0}' WHEN TAR.FSETACCOUNTTYPE='2' THEN N'{1}' WHEN TAR.FSETACCOUNTTYPE='3' THEN N'{2}' ELSE N' ' END  AS FSETACCOUNTTYPE ", "Y", "Z", "C"));
            stringBuilder.AppendLine(string.Format(" FROM (SELECT DISTINCT FORDERID,FDENOID,FOUTID,FRETNOID,FRETURNID,FRECID,FBASERECQTY,FRECPRICEQTY,FBASECAlCQTY,FOPENQTY FROM {0} ) TSO ", this.flowTable));
            stringBuilder.AppendLine(" LEFT JOIN T_AR_RECEIVABLEENTRY TARS ON TARS.FENTRYID=TSO.FRECID ");
            stringBuilder.AppendLine(" LEFT JOIN T_AR_RECEIVABLEENTRY_O TARO ON TARO.FENTRYID=TSO.FRECID ");
            stringBuilder.AppendLine(" LEFT JOIN ");
            stringBuilder.AppendLine(" (select TB1.FTARGETENTRYID,SUM(ISNULL(TB1.FCUROPENAMOUNTFOR,0)) AS FCUROPENAMOUNTFOR from T_AR_BillingMatchLogENTRY TB1 ");
            stringBuilder.AppendLine("  where TB1.FTARGETFROMID='AR_receivable' AND TB1.FISADIBILL='1' group by TB1.FTARGETENTRYID) TARB ");
            stringBuilder.AppendLine("  on TARB.FTARGETENTRYID=TARS.FENTRYID");
            stringBuilder.AppendLine(" LEFT JOIN T_AR_RECEIVABLE TAR ON TAR.FID=TARS.FID ");
            stringBuilder.AppendLine(" LEFT JOIN ");
            stringBuilder.AppendLine(" (select SUM(ISNULL(TIVS.FPRICEQTY,0)) AS FPRICEQTY,SUM(ISNULL(TIVS.FALLAMOUNTFOR,0)) AS FALLAMOUNTFOR,TIVS.FSRCROWID AS FSRCROWID ");
            stringBuilder.AppendLine(string.Format(" FROM  T_IV_SALESICENTRY TIVS INNER JOIN {0} TEMP1 ON TIVS.FSRCROWID=TEMP1.FRECID", this.flowTable));
            stringBuilder.AppendLine(" LEFT JOIN T_IV_SALESIC TIV ON TIV.FID=TIVS.FID AND TIV.FDOCUMENTSTATUS='C' ");
            stringBuilder.AppendLine(" GROUP BY TIVS.FSRCROWID ");
            stringBuilder.AppendLine(" ) TABLETIV ON TABLETIV.FSRCROWID=TSO.FRECID ");
            stringBuilder.AppendLine(" WHERE TSO.FRECID IS NOT NULL  GROUP BY FORDERID,FSETACCOUNTTYPE )T7  ");
            stringBuilder.AppendLine(" ON PD.FENTRYID=T7.FORDERID");
            stringBuilder.AppendLine(" LEFT OUTER JOIN  ");
            stringBuilder.AppendLine(" (Select FSOFId,FORDERID,SUM(FICAMOUNT) as FICAMOUNT,SUM(FNICAMOUNT) as FNICAMOUNT,SUM(FCHARGEOFFAMOUNT) AS FCHARGEOFFAMOUNT From ");
            stringBuilder.AppendLine(" (SELECT Distinct TSO.FSOFId,TSO.FORDERID,TISE.FID");
            stringBuilder.AppendLine(" ,ISNULL(TSO.FBASERECQTY,0)/(case ISNULL(TARO.FSALBASEQTY,1) when  0 then 1 else ISNULL(TARO.FSALBASEQTY,1) end)*ISNULL(TISE.FRECEIVEAMOUNT,0) AS FICAMOUNT");
            stringBuilder.AppendLine(" ,ISNULL(TSO.FBASERECQTY,0)/(case ISNULL(TARO.FSALBASEQTY,1) when  0 then 1 else ISNULL(TARO.FSALBASEQTY,1) end)*ISNULL(TISE.FNRECEIPTAMOUNT,0) AS FNICAMOUNT ");
            stringBuilder.AppendLine(" ,ISNULL(TSO.FBASERECQTY,0)/(case ISNULL(TARO.FSALBASEQTY,1) when  0 then 1 else ISNULL(TARO.FSALBASEQTY,1) end)*ISNULL(TISE.FCHARGEOFFAMOUNT,0) AS FCHARGEOFFAMOUNT ");
            stringBuilder.AppendLine(string.Format(" FROM (SELECT DISTINCT SOE.FID as FSOFId,FORDERID,FRECID ,FBASERECQTY,FBASECAlCQTY FROM {0} t1 inner join T_SAL_ORDERENTRY SOE on t1.FORDERID=SOE.FENTRYID) TSO ", this.flowTable));
            stringBuilder.AppendLine(string.Format(" LEFT  JOIN ( SELECT DISTINCT FID ,FRECID,FALLAMOUNTFOR,FRECEIVEAMOUNT,FNRECEIPTAMOUNT,FROWAMOUNTFOR,FCHARGEOFFAMOUNT FROM {0}) TISE ", this.receivableTable));
            stringBuilder.AppendLine(" ON TSO.FRECID=TISE.FRECID ");
            stringBuilder.AppendLine(" LEFT JOIN T_AR_RECEIVABLEENTRY_O TARO ON TARO.FENTRYID=TSO.FRECID ");
            stringBuilder.AppendLine(" WHERE TISE.FRECID IS NOT NULL ) DistTable  GROUP BY FSOFId,FORDERID )T8 ");
            stringBuilder.AppendLine(" ON (PD.FENTRYID=T8.FORDERID )");
            stringBuilder.AppendLine(this.creatWhereSecond.ToString());
            list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
            DBUtils.ExecuteBatchWithTime(base.Context, list, 300);
            list.Clear();
            stringBuilder.AppendFormat(" IF NOT EXISTS (SELECT 1 FROM KSQL_INDEXES WHERE KSQL_INDNAME = 'idx_{0}_1') CREATE INDEX idx_{0}_1 ON {1} (FORDERID)", this.noCalTable.Substring(3, 22), this.noCalTable);
            list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
            DBUtils.ExecuteBatch(base.Context, list);
            list.Clear();
            if (!this.isRecWithMat)
            {
                stringBuilder.AppendLine(string.Format("/*dialect*/ merge into {0} T0 using\r\n                                (select SumSO.FSOFID,FORDERID,SumSO.FICAMOUNT,FROW\r\n                                  from {0} ca inner join \r\n                                  (select FSOFID,sum(FICAMOUNT) as FICAMOUNT from \r\n                                      (select Distinct ca.FSOFID,ca.FICAMOUNT\r\n                                       from {0} ca inner join {1} flow on ca.FORDERID=flow.FORDERID                                    \r\n                                      where ca.FICAMOUNT is not null) SumSOAmount group by FSOFID\r\n                                  ) SumSO on ca.FSOFID=SumSO.FSOFID) UT\r\n                                on (T0.FROW=UT.FROW)\r\n                                WHEN MATCHED THEN UPDATE SET T0.FICAMOUNT=UT.FICAMOUNT {2}", this.noCalTable, this.flowTable, (base.Context.DatabaseType == DatabaseType.MS_SQL_Server) ? ";" : ""));
                list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
                stringBuilder.Clear();
            }
            stringBuilder.AppendLine(string.Format(" INSERT /*+append*/ INTO {0}", this.tempTableName));
            stringBuilder.AppendLine(" (");
            stringBuilder.AppendLine("  FPRECISION , FPRICEDIGITS, FAMOUNTDIGITS, ");
            stringBuilder.AppendLine("  FCLOSESTATUS,FMRPCLOSESTATUS,FMRPTERMINATESTATUS,");
            stringBuilder.AppendLine("  FSALEORGID ,FSALEORGNUMBER,FSALEORGNAME,FID,FORDERID,FRECID, FBILLNO, FDATE,  ");
            stringBuilder.AppendLine("  FSALEDEPTID,FSALEDEPT,FSALEGROUPID,FSALEGROUP,FSALERID,FSALERNUMBER,FSALES,");
            stringBuilder.AppendLine("  FCUSTID,FCUSTOMERNUMBER,FCUSTOMERNAME,FDELIVERYDAY,");
            stringBuilder.AppendLine("  FMATERIALID ,FMATERIALNUMBER, FMATERIALNAME ,FMATERIALGROUP,FMATERIALGROUPNAME,  ");
            stringBuilder.AppendLine("  FMAPID,FMAPNUMBER,FMAPNAME,");
            stringBuilder.AppendLine("  FSPECIFICATION,FAuxPropId,FAuxProp,   ");
            stringBuilder.AppendLine("  FUNITID,FUNITNAME ,FPRICEUNITID,FPRICEUNITNAME,FCURRENCYID,FMONEYTYPE, FPRICE ,FCHARGE,");
            stringBuilder.AppendLine("  FSOQTY ,FORDERCHARGEQTY, FSOAMOUNT , FSQTY ,FDELIPRICEUNITID, FDELICHARGEQTY,FSAMOUNT , ");
            stringBuilder.AppendLine("  FALREADYQTY ,FOUTPRICEUNITID, FHAVEOUTCHARGEQTY,FALREADYAMOUNT ,FNOTQTY ,FNOTOUTCHARGEQTY, FNOTAMOUNT ,");
            stringBuilder.AppendLine("  FRETURNPQTY ,FRETNOTICEPRICEUNITID,FRETURNPCHARGEQTY, FRETURNPAMOUNT ,");
            stringBuilder.AppendLine("  FRETURNSQTY ,FRETURNPRICEUNITID,FRETURNCHARGEQTY,FRETURNSAMOUNT , ");
            stringBuilder.AppendLine("  FRECQTY ,FRECEPRICEUNITID,FRECECHARGEQTY, FRECAMOUNT , FRECNQTY ,FRECNCHARGEQTY, FRECNAMOUNT,FWRITEOFFAMOUNT ,FSETACCOUNTTYPE,");
            stringBuilder.AppendLine("  FINVOECEQTY ,FINVOECECHARGEQTY, FNVOECEAMOUNT , FNINVOECEQTY ,FNINVOECECHARGEQTY,FNNVOECEAMOUNT ,");
            stringBuilder.AppendLine("   FRECEIPTAMOUNT , FNRECEIPTAMOUNT,FCHARGEOFFAMOUNT, ");
            stringBuilder.AppendLine("   FBASEUNITID_ID,FBASEUNITID,FORDERBASEQTY,FBASEPRICE,FDELIVERYNOTICEQTY,FDELIVERYQTY,FUNDELIVERYQTY,FRETURNNOTICEQTY,FRETURNBASEQTY,FRECEIVEQTY,FUNCONFIRMQTY,FBASEINVOICEQTY,FISRECMAT,");
            stringBuilder.AppendLine("   FROWNO  ");
            stringBuilder.AppendLine(" )");
            stringBuilder.AppendLine(" SELECT ");
            stringBuilder.AppendLine(" TBUT.FPRECISION,");
            stringBuilder.AppendLine(" TBCU.FPRICEDIGITS,");
            stringBuilder.AppendLine(" TBCU.FAMOUNTDIGITS,");
            stringBuilder.AppendLine(" CASE WHEN TSO.FCLOSESTATUS='B' THEN  'Y'  ELSE ' ' END,");
            stringBuilder.AppendLine(" CASE WHEN TSOE.FMRPCLOSESTATUS='B' THEN  'Y'  ELSE ' ' END,");
            stringBuilder.AppendLine(" CASE WHEN TSOE.FMRPTERMINATESTATUS='B' THEN  'Y'  ELSE 'N' END,");
            stringBuilder.AppendLine(" TSO.FSALEORGID,");
            stringBuilder.AppendLine(" TORG.FNUMBER AS FSALEORGNUMBER,");
            stringBuilder.AppendLine(" TORGL.FNAME AS FSALEORGNAME,");
            stringBuilder.AppendLine(" TSO.FID AS FID, ");
            stringBuilder.AppendLine(" TSOE.FENTRYID  AS FORDERID, ");
            stringBuilder.AppendLine(" T1.FRECID AS FRECID, ");
            stringBuilder.AppendLine(" TSO.FBILLNO AS FBILLNO, ");
            stringBuilder.AppendLine(" TSO.FDATE AS FDATE, ");
            stringBuilder.AppendLine(" TSO.FSALEDEPTID,");
            stringBuilder.AppendLine(" TBDL.FNAME AS DEPTNAME,");
            stringBuilder.AppendLine(" TSO.FSALEGROUPID,");
            stringBuilder.AppendLine(" VBOL.FNAME AS FSALEGROUP,");
            stringBuilder.AppendLine(" TSO.FSALERID, ");
            stringBuilder.AppendLine(" VBS.FNUMBER AS FSALENUMBER,");
            stringBuilder.AppendLine(" VBSL.FNAME AS FSALES,");
            stringBuilder.AppendLine(" TSO.FCUSTID,");
            stringBuilder.AppendLine(" TBCE.FNUMBER AS FCUSTOMERID,");
            stringBuilder.AppendLine(" TBC.FNAME AS FCUSTOMERNAME,  ");
            stringBuilder.AppendLine(" TSRD.FDELIVERYDATE AS FDELIVERYDATE,");
            stringBuilder.AppendLine(" TSOE.FMATERIALID,");
            stringBuilder.AppendLine(" TBMN.FNUMBER AS FMATERIALNUMBER, ");
            stringBuilder.AppendLine(" TBM.FNAME AS FMATERIALNAME,  ");
            stringBuilder.AppendLine(" TBMN.FMATERIALGROUP,TGOU.FNAME FMATERIALGROUPNAME,");
            stringBuilder.AppendLine(" TSOE.FMAPID,");
            stringBuilder.AppendLine(" VCM.FNUMBER AS FMAPNUMBER, ");
            stringBuilder.AppendLine(" VCML.FNAME AS FMAPNAME,  ");
            stringBuilder.AppendLine(" TBM.FSPECIFICATION as FSPECIFICATION,");
            stringBuilder.AppendLine(" TSOE.FAuxPropId as FAuxPropId,");
            stringBuilder.AppendLine(" '' as FAuxProp,");
            stringBuilder.AppendLine(" TSOE.FUNITID AS FUNITID, ");
            stringBuilder.AppendLine(" TBU.FNAME AS FUNITNAME, ");
            stringBuilder.AppendLine(" TSOF.FPRICEUNITID, ");
            stringBuilder.AppendLine(" TBU2.FNAME AS FPRICEUNITNAME, ");
            stringBuilder.AppendLine(" TSF.FSETTLECURRID,");
            stringBuilder.AppendLine(" TBCL.FNAME AS FMONEYTYPE,");
            stringBuilder.AppendLine(" TSOF.FALLAMOUNT/(CASE WHEN TSOE.FBASEUNITQTY=0 THEN  1 WHEN T1.FORDERQTY*TSOE.FQTY/TSOE.FBASEUNITQTY=0 THEN  1  ELSE T1.FORDERQTY*TSOE.FQTY/TSOE.FBASEUNITQTY END) AS FPRICE,");
            stringBuilder.AppendLine(" TSOF.FALLAMOUNT/(CASE WHEN ISNULL(T1.FORDERCHARGEQTY,0) = 0 THEN NULL ELSE T1.FORDERCHARGEQTY END) AS FCHARGE,");
            stringBuilder.AppendLine(" T1.FORDERQTY*(CASE WHEN TSOE.FBASEUNITQTY=0 THEN 1 ELSE TSOE.FQTY/TSOE.FBASEUNITQTY END) AS FSOQTY, ");
            stringBuilder.AppendLine(" T1.FORDERCHARGEQTY AS FORDERCHARGEQTY,");
            stringBuilder.AppendLine("  TSOF.FALLAMOUNT AS FSOAMOUNT, ");
            stringBuilder.AppendLine(" T1.FDELIVERQTY*(CASE WHEN TSOE.FBASEUNITQTY=0 THEN 1 ELSE TSOE.FQTY/TSOE.FBASEUNITQTY END) AS FSQTY, ");
            stringBuilder.AppendLine(" T1.FDELIPRICEUNITID AS FDELIPRICEUNITID,");
            stringBuilder.AppendLine(" T1.FDELICHARGEQTY AS FDELICHARGEQTY, ");
            stringBuilder.AppendLine(" T1.FDELIVERQTY*TSOF.FALLAMOUNT/(CASE WHEN ISNULL(T1.FORDERQTY,0)=0  THEN NULL ELSE  T1.FORDERQTY END) AS FSAMOUNT, ");
            stringBuilder.AppendLine(" (case when TSO.FBUSINESSTYPE = 'STRAIGHT' then 0 else(T1.FOUTQTY*(CASE WHEN TSOE.FBASEUNITQTY=0 THEN 1 ELSE TSOE.FQTY/TSOE.FBASEUNITQTY END)) end)AS FALREADYQTY, ");
            stringBuilder.AppendLine(" T1.FOUTPRICEUNITID AS FOUTPRICEUNITID, ");
            stringBuilder.AppendLine(" (case when TSO.FBUSINESSTYPE = 'STRAIGHT' then 0 else T1.FOUTCHARGEQTY END) AS FHAVEOUTCHARGEQTY, ");
            stringBuilder.AppendLine(" (case when TSO.FBUSINESSTYPE = 'STRAIGHT' then 0 else(T1.FOUTQTY*TSOF.FALLAMOUNT/(CASE WHEN ISNULL(T1.FORDERQTY,0)=0  THEN NULL ELSE  T1.FORDERQTY END)) end) AS FALREADYAMOUNT, ");
            stringBuilder.AppendLine(" (CASE WHEN TSOE.FMRPTERMINATESTATUS='B' THEN  0  ELSE (case when TSO.FBUSINESSTYPE = 'STRAIGHT' then 0 else(T1.FREMAINOUTQTY*(CASE WHEN TSOE.FBASEUNITQTY=0 THEN 1 ELSE TSOE.FQTY/TSOE.FBASEUNITQTY END)) end) END) AS FNOTQTY, ");
            stringBuilder.AppendLine(" (CASE WHEN TSOE.FMRPTERMINATESTATUS='B' THEN  0  ELSE (case when TSO.FBUSINESSTYPE = 'STRAIGHT' then 0 else T1.FNOTOUTCHARGEQTY END) END) AS FNOTOUTCHARGEQTY, ");
            stringBuilder.AppendLine(" (CASE WHEN TSOE.FMRPTERMINATESTATUS='B' THEN  0  ELSE (case when TSO.FBUSINESSTYPE = 'STRAIGHT' then 0 else(T1.FREMAINOUTQTY*TSOF.FALLAMOUNT/(CASE WHEN ISNULL(T1.FORDERQTY,0)=0  THEN NULL ELSE  T1.FORDERQTY END)) end) END) AS FNOTAMOUNT, ");
            stringBuilder.AppendLine(" T1.FRETNOTICEQTY*(CASE WHEN TSOE.FBASEUNITQTY=0 THEN 1 ELSE TSOE.FQTY/TSOE.FBASEUNITQTY END) AS FRETURNPQTY, ");
            stringBuilder.AppendLine(" T1.FRETNOTICEPRICEUNITID AS FRETNOTICEPRICEUNITID,");
            stringBuilder.AppendLine(" T1.FRETNOTICECHARGEQTY AS FRETURNPCHARGEQTY, ");
            stringBuilder.AppendLine(" T1.FRETNOTICEQTY*TSOF.FALLAMOUNT/(CASE WHEN ISNULL(T1.FORDERQTY,0)=0  THEN NULL ELSE  T1.FORDERQTY END) AS FRETURNPAMOUNT, ");
            stringBuilder.AppendLine(" T1.FRETURNQTY*(CASE WHEN TSOE.FBASEUNITQTY=0 THEN 1 ELSE TSOE.FQTY/TSOE.FBASEUNITQTY END) AS FRETURNSQTY, ");
            stringBuilder.AppendLine(" T1.FRETURNPRICEUNITID AS FRETURNPRICEUNITID,");
            stringBuilder.AppendLine(" T1.FRETURNCHARGEQTY AS FRETURNCHARGEQTY, ");
            stringBuilder.AppendLine(" T1.FRETURNQTY*TSOF.FALLAMOUNT/(CASE WHEN ISNULL(T1.FORDERQTY,0)=0  THEN NULL ELSE  T1.FORDERQTY END) AS FRETURNSAMOUNT, ");
            stringBuilder.AppendLine(" T1.FARQTY*(CASE WHEN TSOE.FBASEUNITQTY=0 THEN 1 ELSE TSOE.FQTY/TSOE.FBASEUNITQTY END) AS FRECQTY, ");
            stringBuilder.AppendLine(" T1.FARPRICEUNITID AS FRECEPRICEUNITID, ");
            stringBuilder.AppendLine(" T1.FARCHARGEQTY AS FRECECHARGEQTY, ");
            stringBuilder.AppendLine(" ISNULL(T1.FARAMOUNT,0) AS FRECAMOUNT,");
            stringBuilder.AppendLine(" (CASE WHEN (ISNULL(ABS(T1.FORDERQTY),0)-ISNULL(ABS(T1.FARQTY),0))<=0  THEN 0 ELSE T1.FORDERQTY-ISNULL(T1.FARQTY,0) END )*(CASE WHEN TSOE.FBASEUNITQTY=0 THEN 1 ELSE TSOE.FQTY/TSOE.FBASEUNITQTY END) AS FRECNQTY, ");
            stringBuilder.AppendLine(" CASE WHEN (ISNULL(ABS(TSOF.FALLAMOUNT),0)-ISNULL(ABS(T1.FARAMOUNT),0))<=0 THEN 0 ELSE ISNULL(TSOF.FALLAMOUNT,0)-ISNULL(T1.FARAMOUNT,0) END AS FRECNAMOUNT, ");
            stringBuilder.AppendLine(" 0 as FRECNCHARGEQTY,");
            stringBuilder.AppendLine(" ISNULL(T1.FWRITEOFFAMOUNT,0) AS FWRITEOFFAMOUNT,");
            stringBuilder.AppendLine(string.Format(" CASE WHEN T1.FSETACCOUNTTYPE='Y' THEN N'{0}' WHEN T1.FSETACCOUNTTYPE='Z' THEN N'{1}' WHEN T1.FSETACCOUNTTYPE='C' THEN N'{2}' ELSE N' ' END  AS FSETACCOUNTTYPE, ", ResManager.LoadKDString("业务应收", "004102000033546", SubSystemType.SCM, new object[0]), ResManager.LoadKDString("暂估应收", "004102000033547", SubSystemType.SCM, new object[0]), ResManager.LoadKDString("财务应收", "004102000033545", SubSystemType.SCM, new object[0])));
            stringBuilder.AppendLine(" (CASE WHEN T1.FSETACCOUNTTYPE='Y' THEN ISNULL(T1.FIQTY,0)*(CASE WHEN TSOE.FBASEUNITQTY=0 THEN 1 ELSE TSOE.FQTY/TSOE.FBASEUNITQTY END) WHEN T1.FSETACCOUNTTYPE='C' THEN ISNULL(T1.FIQTY,0) ELSE 0 END) AS FINVOECEQTY, ");
            stringBuilder.AppendLine(" ISNULL(T1.FICHARGEQTY,0) AS FINVOECECHARGEQTY, ");
            stringBuilder.AppendLine(" ISNULL(T1.FIAMOUNT,0) AS FNVOECEAMOUNT, ");
            stringBuilder.AppendLine("(CASE WHEN T1.FSETACCOUNTTYPE='Y' THEN (CASE WHEN (ISNULL(ABS(T1.FARQTY),0)-ISNULL(ABS(T1.FIQTY),0))<=0   THEN 0 ELSE ISNULL(T1.FARQTY,0)-ISNULL(T1.FIQTY,0) END )*(CASE WHEN TSOE.FBASEUNITQTY=0 THEN 1 ELSE TSOE.FQTY/TSOE.FBASEUNITQTY END)\r\n                                WHEN T1.FSETACCOUNTTYPE='C' THEN (T1.FARQTY*(CASE WHEN TSOE.FBASEUNITQTY=0 THEN 1 ELSE TSOE.FQTY/TSOE.FBASEUNITQTY END)-ISNULL(T1.FIQTY,0)) ELSE 0 END) AS FNINVOECEQTY, ");
            stringBuilder.AppendLine("(CASE WHEN T1.FSETACCOUNTTYPE='Y' THEN (CASE WHEN (ISNULL(ABS(T1.FARQTY),0)-ISNULL(ABS(T1.FIQTY),0))<=0   THEN 0 ELSE ISNULL(T1.FARQTY,0)-ISNULL(T1.FIQTY,0) END )*(CASE WHEN TSOF.FPRICEBASEQTY=0 THEN 1 ELSE TSOF.FPRICEUNITQTY/TSOF.FPRICEBASEQTY END)\r\n                                WHEN T1.FSETACCOUNTTYPE='C' THEN \r\n                                (CASE WHEN (ISNULL(ABS(T1.FARQTY),0)-ISNULL(ABS(T1.FIQTY*(CASE WHEN TSOE.FQTY=0 THEN 1 ELSE TSOE.FBASEUNITQTY/TSOE.FQTY END)),0))<=0 THEN 0 ELSE ISNULL(T1.FARQTY,0)-ISNULL(T1.FIQTY*(CASE WHEN TSOE.FQTY=0 THEN 1 ELSE TSOE.FBASEUNITQTY/TSOE.FQTY END),0) END )*(CASE WHEN TSOF.FPRICEBASEQTY=0 THEN 1 ELSE TSOF.FPRICEUNITQTY/TSOF.FPRICEBASEQTY END)\r\n                                ELSE 0 END) AS FNINVOECECHARGEQTY, ");
            stringBuilder.AppendLine("(CASE WHEN T1.FSETACCOUNTTYPE='Z' THEN  0 ELSE (CASE WHEN ( ISNULL(ABS(T1.FARAMOUNT),0) -ISNULL(ABS(T1.FIAMOUNT),0))<=0 THEN 0 ELSE  (ISNULL(T1.FARAMOUNT,0) -ISNULL(T1.FIAMOUNT,0))+(CASE WHEN ISNULL(T1.FWRITEOFFAMOUNT,0)>0 THEN 0 ELSE ISNULL(T1.FWRITEOFFAMOUNT,0) END) END ) END) AS FNNVOECEAMOUNT,  ");
            if (this.isRecWithMat)
            {
                stringBuilder.AppendLine(" (CASE WHEN T1.FSETACCOUNTTYPE='Z' THEN 0 ELSE ISNULL(TARS.FRECEIVEAMOUNT,0) END) AS FRECEIPTAMOUNT, ");
            }
            else
            {
                stringBuilder.AppendLine(" (CASE WHEN T1.FSETACCOUNTTYPE='Z' THEN 0 ELSE ISNULL(T1.FICAMOUNT,0) END) AS FRECEIPTAMOUNT, ");
            }
            stringBuilder.AppendLine(" (CASE WHEN T1.FSETACCOUNTTYPE='Z' THEN 0 ELSE ISNULL(T1.FNICAMOUNT,0) END) AS FNRECEIPTAMOUNT, ");
            if (this.isRecWithMat)
            {
                stringBuilder.AppendLine(" 0 AS FCHARGEOFFAMOUNT, ");
            }
            else
            {
                stringBuilder.AppendLine(" (CASE WHEN T1.FSETACCOUNTTYPE='Z' THEN 0 ELSE ROUND(T1.FCHARGEOFFAMOUNT,6) END) AS FCHARGEOFFAMOUNT, ");
            }
            stringBuilder.AppendLine(" TSOE.FBASEUNITID,TBU3.FNAME AS FBASEUNITNAME, ");
            stringBuilder.AppendLine(" T1.FORDERBASEQTY, ");
            stringBuilder.AppendLine(" T1.FBASEPRICE,  ");
            stringBuilder.AppendLine(" T1.FDELIVERYNOTICEQTY, ");
            stringBuilder.AppendLine(" T1.FDELIVERYQTY, ");
            stringBuilder.AppendLine(" T1.FUNDELIVERYQTY, ");
            stringBuilder.AppendLine(" T1.FRETURNNOTICEQTY, ");
            stringBuilder.AppendLine(" T1.FRETURNBASEQTY, ");
            stringBuilder.AppendLine(" T1.FRECEIVEQTY, ");
            stringBuilder.AppendLine(" T1.FUNCONFIRMQTY, ");
            stringBuilder.AppendLine(" T1.FBASEINVOICEQTY, ");
            stringBuilder.AppendLine(" (CASE WHEN ISNULL(TBR.FRECMETHOD,0) <> 3 THEN 0 ELSE 1 END) AS FISRECMAT,");
            stringBuilder.AppendLine(" ROW_NUMBER() OVER (ORDER BY TSO.FID,TSO.FDATE ASC ,TSOE.FENTRYID   ) ");
            stringBuilder.AppendLine(string.Format(" FROM {0} T1 ", this.noCalTable));
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_SAL_ORDERENTRY  TSOE  ");
            stringBuilder.AppendLine(" ON T1.FORDERID=TSOE.FENTRYID ");
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_SAL_ORDER TSO  ");
            stringBuilder.AppendLine(" ON TSOE.FID=TSO.FID");
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_SAL_ORDERENTRY_F TSOF ");
            stringBuilder.AppendLine(" ON T1.FORDERID=TSOF.FENTRYID ");
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_AR_RECEIVABLEENTRY TARS ON TARS.FENTRYID=T1.FRECID ");
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_BD_CUSTOMER_L TBC  ");
            stringBuilder.AppendLine(string.Format(" ON TSO.FCUSTID=TBC.FCUSTID  AND TBC.FLOCALEID={0} ", base.Context.UserLocale.LCID.ToString()));
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_BD_MATERIAL  TBMN  ");
            stringBuilder.AppendLine(" ON TSOE.FMATERIALID=TBMN.FMATERIALID");
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_BD_MATERIALSALE TBSU  ");
            stringBuilder.AppendLine(" ON TBSU.FMATERIALID=TSOE.FMATERIALID ");
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_BD_MATERIALBASE TBSE  ");
            stringBuilder.AppendLine(" ON TBSE.FMATERIALID=TSOE.FMATERIALID ");
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_BD_MATERIAL_L TBM ");
            stringBuilder.AppendLine(string.Format(" ON TBM.FMATERIALID=TSOE.FMATERIALID AND TBM.FLOCALEID={0} ", base.Context.UserLocale.LCID.ToString()));
            stringBuilder.AppendLine(" LEFT  JOIN V_SAL_CUSTMATMAPPING VCM  ");
            stringBuilder.AppendLine(" ON TSOE.FMAPID=VCM.FID ");
            stringBuilder.AppendLine(" LEFT  JOIN V_SAL_CUSTMATMAPPING_L VCML  ");
            stringBuilder.AppendLine(" ON VCM.FID=VCML.FID ");
            stringBuilder.AppendLine(string.Format(" LEFT JOIN T_BD_MATERIALGROUP_L TGOU ON TGOU.FID=TBMN.FMATERIALGROUP AND TGOU.FLOCALEID={0} ", base.Context.UserLocale.LCID));
            stringBuilder.AppendLine(" LEFT OUTER JOIN t_BD_Unit_L TBU  ");
            stringBuilder.AppendLine(string.Format(" ON TBU.FUNITID=TSOE.FUNITID  AND  TBU.FLOCALEID={0} ", base.Context.UserLocale.LCID.ToString()));
            stringBuilder.AppendLine(" LEFT  JOIN t_BD_Unit_L TBU2  ");
            stringBuilder.AppendLine(string.Format(" ON TBU2.FUNITID=TSOF.FPRICEUNITID AND  TBU2.FLOCALEID={0} ", base.Context.UserLocale.LCID.ToString()));
            stringBuilder.AppendLine(" LEFT  JOIN t_BD_Unit_L TBU3  ");
            stringBuilder.AppendLine(string.Format(" ON TBU3.FUNITID=TSOE.FBASEUNITID AND  TBU3.FLOCALEID={0} ", base.Context.UserLocale.LCID.ToString()));
            stringBuilder.AppendLine("  LEFT OUTER JOIN V_BD_SALESMAN_L VBSL  ");
            stringBuilder.AppendLine(string.Format(" ON TSO.FSALERID=VBSL.FID AND VBSL.FLOCALEID={0} ", base.Context.UserLocale.LCID.ToString()));
            stringBuilder.AppendLine(" LEFT OUTER JOIN V_BD_SALESMAN VBS ");
            stringBuilder.AppendLine(" ON TSO.FSALERID=VBS.FID ");
            stringBuilder.AppendLine(" LEFT OUTER JOIN V_BD_OPERATORGROUP_L VBOL  ");
            stringBuilder.AppendLine(string.Format(" ON TSO.FSALEGROUPID=VBOL.FENTRYID AND VBOL.FLOCALEID={0} ", base.Context.UserLocale.LCID.ToString()));
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_SAL_ORDERFIN  TSF  ");
            stringBuilder.AppendLine(" ON TSO.FID=TSF.FID");
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_BD_CURRENCY_L TBCL ");
            stringBuilder.AppendLine(string.Format(" ON TSF.FSETTLECURRID=TBCL.FCURRENCYID AND TBCL.FLOCALEID={0}", base.Context.UserLocale.LCID.ToString()));
            stringBuilder.AppendLine(" LEFT JOIN T_BD_RECCONDITION TBR ");
            stringBuilder.AppendLine(string.Format(" ON TSF.FRECCONDITIONID=TBR.FID ", new object[0]));
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_BD_CUSTOMER  TBCE  ");
            stringBuilder.AppendLine(" ON TSO.FCUSTID=TBCE.FCUSTID");
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_BD_DEPARTMENT_L TBDL ");
            stringBuilder.AppendLine(string.Format(" ON TSO.FSALEDEPTID=TBDL.FDEPTID AND TBDL.FLOCALEID={0} ", base.Context.UserLocale.LCID.ToString()));
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_BD_DEPARTMENT TBD ");
            stringBuilder.AppendLine(" ON TSO.FSALEDEPTID=TBD.FDEPTID");
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_SAL_ORDERENTRY_D TSRD  ");
            stringBuilder.AppendLine(" ON T1.FORDERID=TSRD.FENTRYID ");
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_ORG_ORGANIZATIONS  TORG ");
            stringBuilder.AppendLine(" ON TSO.FSALEORGID=TORG.FORGID ");
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_ORG_ORGANIZATIONS_L  TORGL ");
            stringBuilder.AppendLine(string.Format(" ON TSO.FSALEORGID=TORGL.FORGID AND TORGL.FLOCALEID={0} ", base.Context.UserLocale.LCID.ToString()));
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_BD_CURRENCY TBCU ");
            stringBuilder.AppendLine(" ON TSF.FSETTLECURRID=TBCU.FCURRENCYID");
            stringBuilder.AppendLine(" LEFT OUTER JOIN T_BD_UNIT TBUT ");
            stringBuilder.AppendLine(" ON TBSE.FBASEUNITID=TBUT.FUNITID");
            stringBuilder.AppendLine(" WHERE TSO.FID IS NOT NULL ");
            if (this.creatWhereThrid.ToString().Trim() != string.Empty)
            {
                stringBuilder.AppendLine(" AND ");
                stringBuilder.AppendLine(this.creatWhereThrid.ToString());
            }
            stringBuilder.AppendLine(this.orderBy.ToString());
            stringBuilder.AppendLine(" ORDER BY TSO.FID,TSO.FDATE ASC ,TSOE.FENTRYID ");
            list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
            DBUtils.ExecuteBatchWithTime(base.Context, list, 300);
            list = new List<SqlObject>();
            this.MergePriceQTY(list);
            stringBuilder.AppendLine(string.Format("/*dialect*/MERGE INTO {0} T11\r\n                                             USING (\r\n\t                                                SELECT FID,FORDERID,FBILLNO,FMATERIALID\r\n                                                    ,SUM(FDELICHARGEQTY) AS FALLDELICHARGEQTY\r\n                                                    ,SUM(FHAVEOUTCHARGEQTY) AS FALLHAVEOUTCHARGEQTY\r\n                                                    ,SUM(FRETURNPCHARGEQTY) AS FALLRETURNPCHARGEQTY\r\n                                                    ,SUM(FRETURNCHARGEQTY) AS FALLRETURNCHARGEQTY\r\n                                                    ,SUM(FRECECHARGEQTY) AS FALLRECECHARGEQTY\r\n\t                                            FROM {0} GROUP BY FID,FORDERID,FBILLNO,FMATERIALID) T12\r\n\t                                            ON (T11.FID = T12.FID AND T11.FORDERID = T12.FORDERID AND T11.FBILLNO = T12.FBILLNO  AND T11.FMATERIALID = T12.FMATERIALID)\r\n                                             WHEN MATCHED THEN UPDATE\r\n\t                                            \tSET T11.FDELICHARGEQTY = T12.FALLDELICHARGEQTY\r\n\t                                            \t,T11.FHAVEOUTCHARGEQTY = T12.FALLHAVEOUTCHARGEQTY\r\n\t                                            \t,T11.FRETURNPCHARGEQTY = T12.FALLRETURNPCHARGEQTY\r\n\t                                            \t,T11.FRETURNCHARGEQTY = T12.FALLRETURNCHARGEQTY\r\n                                                    ,T11.FRECECHARGEQTY = T12.FALLRECECHARGEQTY {1}", this.tempTableName, (base.Context.DatabaseType == DatabaseType.MS_SQL_Server) ? ";" : ""));
            list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
            if (this.isRecWithMat)
            {
                stringBuilder.AppendLine(string.Format("MERGE INTO {0} T11\r\n                                             USING ( SELECT A.FID,A.FORDERID,A.FROWNO,B.FRECEIPTAMOUNT,B.FWRITEOFFAMOUNT,B.FSETACCOUNTTYPE FROM \r\n                                                    (SELECT FID,FORDERID,MIN(FROWNO) AS FROWNO FROM {0} GROUP BY FID,FORDERID) A INNER JOIN \r\n\t                                                (SELECT FID,FORDERID,FSETACCOUNTTYPE,SUM(FRECEIPTAMOUNT) AS FRECEIPTAMOUNT,MIN(FWRITEOFFAMOUNT) AS FWRITEOFFAMOUNT FROM {0} GROUP BY FID,FORDERID,FSETACCOUNTTYPE) B ON A.FID = B.FID AND A.FORDERID = B.FORDERID \r\n                                                    ) T12 ON (T11.FID = T12.FID AND T11.FORDERID = T12.FORDERID AND T11.FROWNO = T12.FROWNO)\r\n                                             WHEN MATCHED THEN UPDATE\r\n\t                                            \tSET T11.FRECEIPTAMOUNT = (CASE WHEN T12.FSETACCOUNTTYPE='{1}' THEN 0 \r\n                                            ELSE (T12.FRECEIPTAMOUNT+(CASE WHEN ISNULL(T12.FWRITEOFFAMOUNT,0)>0 THEN 0 ELSE ISNULL(T12.FWRITEOFFAMOUNT,0) END)) END)", this.tempTableName, ResManager.LoadKDString("暂估应收", "004102000033547", SubSystemType.SCM, new object[0])));
                list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
                stringBuilder.Clear();
            }
            stringBuilder.AppendLine(string.Format("DELETE FROM {0}\r\n                                             WHERE FBILLNO IN (SELECT DISTINCT FBILLNO FROM {0} GROUP BY FID,FORDERID,FBILLNO,FMATERIALID  HAVING COUNT(1) > 1) \r\n                                             AND FROWNO NOT IN (SELECT MIN(FROWNO) FROM {0} GROUP BY FID,FORDERID,FBILLNO,FMATERIALID HAVING COUNT(1)>=1)", this.tempTableName));
            list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
            stringBuilder.AppendLine(string.Format("/*dialect*/ UPDATE {0} SET FNOTOUTCHARGEQTY = \r\n            (CASE WHEN FMRPTERMINATESTATUS='Y' THEN  0  ELSE ( CASE WHEN FORDERCHARGEQTY - {1}(FHAVEOUTCHARGEQTY,0)<= 0 THEN 0 ELSE FORDERCHARGEQTY - {1}(FHAVEOUTCHARGEQTY,0) END) END),\r\n            FRECNCHARGEQTY = ( CASE WHEN FORDERCHARGEQTY - {1}(FRECECHARGEQTY,0)<= 0 THEN 0 ELSE FORDERCHARGEQTY - {1}(FRECECHARGEQTY,0) END)", this.tempTableName, (base.Context.DatabaseType == DatabaseType.MS_SQL_Server) ? "ISNULL" : "NVL"));
            list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
            if (this.isRecWithMat)
            {
                stringBuilder.AppendLine(string.Format(" MERGE INTO {0} T0 USING\r\n                           (\r\n                            SELECT Distinct srctb.FOrderId,(CASE WHEN FSETACCOUNTTYPE=N'{1}' THEN 0 ELSE FSumRecAmount-(ISNULL(FRECEIPTAMOUNT,0)+ISNULL(FCHARGEOFFAMOUNT,0)) END) as FNRECEIPTAMOUNT  FROM {0} srctb \r\n                                INNER JOIN  (SELECT FOrderId, SUM(ISNULL(FRECAMOUNT,0) + ISNULL(FWRITEOFFAMOUNT , 0)) as FSumRecAmount  FROM {0} GROUP BY FOrderId) sumtb ON srctb.FOrderId=sumtb.FOrderId\r\n                            ) UT ON (T0.FOrderId=UT.FOrderId)\r\n                            WHEN MATCHED THEN UPDATE SET T0.FNRECEIPTAMOUNT=UT.FNRECEIPTAMOUNT", this.tempTableName, ResManager.LoadKDString("暂估应收", "004102000033547", SubSystemType.SCM, new object[0])));
            }
            else
            {
                stringBuilder.AppendLine(string.Format(" MERGE INTO {0} T0 USING\r\n                           (\r\n                            SELECT srctb.FID,srctb.FOrderId,(CASE WHEN FSETACCOUNTTYPE=N'{1}' THEN 0 ELSE FSumRecAmount-(ISNULL(FRECEIPTAMOUNT,0)+ISNULL(FCHARGEOFFAMOUNT,0)) END) as FNRECEIPTAMOUNT  FROM {0} srctb \r\n                                INNER JOIN  (SELECT Fid, SUM(ISNULL(FRECAMOUNT,0) + ISNULL(FWRITEOFFAMOUNT , 0)) as FSumRecAmount  FROM {0} GROUP BY FID) sumtb ON srctb.fid=sumtb.fid\r\n                            ) UT ON (T0.FID=UT.FID AND T0.FOrderId=UT.FOrderId)\r\n                            WHEN MATCHED THEN UPDATE SET T0.FNRECEIPTAMOUNT=UT.FNRECEIPTAMOUNT", this.tempTableName, ResManager.LoadKDString("暂估应收", "004102000033547", SubSystemType.SCM, new object[0])));
            }
            list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
            stringBuilder.AppendLine(string.Format("/*dialect*/ MERGE INTO {0} T0 USING\r\n                               (\r\n                                SELECT NT.FOrderEntryId,SUM(NT.FADVANCEAMOUNT) AS FADVANCEAMOUNT FROM \r\n                                (SELECT TSOP.FOrderEntryId,TSOPE.FAMOUNT AS FADVANCEAMOUNT FROM {0} TM\r\n                                INNER JOIN T_SAL_ORDERPLAN TSOP ON TM.FISRECMAT = 1 AND TM.FOrderId = TSOP.FOrderEntryId AND TSOP.FNEEDRECADVANCE = 1\r\n                                INNER JOIN T_SAL_ORDERPLANENTRY TSOPE ON TSOPE.FENTRYID = TSOP.FENTRYID) NT\r\n                                GROUP BY NT.FOrderEntryId\r\n                              ) UT ON (T0.FOrderId = UT.FOrderEntryId)\r\n                            WHEN MATCHED THEN UPDATE SET T0.FADVANCEAMOUNT = UT.FADVANCEAMOUNT {1}", this.tempTableName, (base.Context.DatabaseType == DatabaseType.MS_SQL_Server) ? ";" : ""));
            list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
            stringBuilder.AppendLine(string.Format("/*dialect*/ MERGE INTO {0} T0 USING\r\n                               (\r\n                                SELECT NT.FID,SUM(NT.FADVANCEAMOUNT) AS FADVANCEAMOUNT FROM \r\n                                (SELECT DISTINCT TSO.FID,TSOPE.FENTRYID,TSOPE.FADVANCEENTRYID,TSOPE.FAMOUNT AS FADVANCEAMOUNT FROM {0} TM\r\n                                INNER JOIN T_SAL_ORDER TSO ON TSO.FID = TM.FID AND TM.FISRECMAT = 0 \r\n                                INNER JOIN T_SAL_ORDERPLAN TSOP ON TSO.FID = TSOP.FID AND TSOP.FNEEDRECADVANCE = 1\r\n                                INNER JOIN T_SAL_ORDERPLANENTRY TSOPE ON TSOPE.FENTRYID = TSOP.FENTRYID) NT\r\n                                GROUP BY NT.FID\r\n                              ) UT ON (T0.FID = UT.FID)\r\n                            WHEN MATCHED THEN UPDATE SET T0.FADVANCEAMOUNT = UT.FADVANCEAMOUNT {1}", this.tempTableName, (base.Context.DatabaseType == DatabaseType.MS_SQL_Server) ? ";" : ""));
            list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
            stringBuilder.AppendLine(string.Format("/*dialect*/ merge into {0} T0 using\r\n                                (select OT.FRowNo,FISRECMAT from {0} OT\r\n                                    where not Exists\r\n                                    (select 1 from  (Select MIN(FROWNO) as FROWNO from {0} group by FID) ET where ET.FROWNO=OT.FRowNo)) UT\r\n                                on (T0.FROWNO=UT.FROWNO  AND T0.FISRECMAT = 0)\r\n                                WHEN MATCHED THEN UPDATE SET  FADVANCEAMOUNT = 0 {1}", this.tempTableName, (base.Context.DatabaseType == DatabaseType.MS_SQL_Server) ? ";" : ""));
            list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
            if (!this.isRecWithMat)
            {
                stringBuilder.AppendLine(string.Format("/*dialect*/ merge into {0} T0 using\r\n                                (select OT.FRowNo, 0 as FRECEIPTAMOUNT,0 as FNRECEIPTAMOUNT,0 as FCHARGEOFFAMOUNT from {0} OT\r\n                                    where not Exists\r\n                                    (select 1 from  (Select MIN(FROWNO) as FROWNO from {0} group by FID) ET where ET.FROWNO=OT.FRowNo)) UT\r\n                                on (T0.FROWNO=UT.FROWNO)\r\n                                WHEN MATCHED THEN UPDATE SET T0.FRECEIPTAMOUNT=UT.FRECEIPTAMOUNT,T0.FNRECEIPTAMOUNT=UT.FNRECEIPTAMOUNT,T0.FCHARGEOFFAMOUNT=UT.FCHARGEOFFAMOUNT {1}", this.tempTableName, (base.Context.DatabaseType == DatabaseType.MS_SQL_Server) ? ";" : ""));
                list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
                stringBuilder.Clear();
            }
            if (!string.IsNullOrWhiteSpace(this.priceFrom) && this.priceFrom.Equals("RECEIVEBILL", StringComparison.CurrentCultureIgnoreCase))
            {
                stringBuilder.AppendFormat("\r\n                                    merge into {0} ts \r\n                                    using\r\n                                    (\r\n                                        select tsd.FORDERID forderid,\r\n                                        max(recard.fentryid) frecid,\r\n                                        max(recardo.fsalunitid) funitid,\r\n                                        (case when sum(recardo.FSalQTY)=0 then   convert(decimal(18,8),sum( recard.FALLAMOUNTFOR)) \r\n                                             else convert(decimal(18,8),sum( recard.FALLAMOUNTFOR))/sum(recardo.FSalQTY) end) as  fprice,--处理精度不准的问题\r\n                                        (case when sum(recard.FPRICEQTY)=0 then   convert(decimal(18,8),sum( recard.FALLAMOUNTFOR))\r\n                                             else convert(decimal(18,8),sum( recard.FALLAMOUNTFOR))/sum(recard.FPRICEQTY)  end) as fcharge,--处理精度不准的问题\r\n                                        max(recar.fcurrencyid)  fcurrencyid,\r\n                                        max(tcur.FNAME) fcurrencyname\r\n                                       \r\n                                        from T_AR_RECEIVABLE recar\r\n                                        inner join t_ar_receivableentry recard on recar.fid=recard.fid \r\n                                        inner join t_ar_receivableentry_o recardo on recard.fentryid=recardo.fentryid\r\n                                        inner join {1} tsd on tsd.frecid = recard.fentryid\r\n                                        left join T_BD_CURRENCY_L tcur on tcur.FCURRENCYID=recar.fcurrencyid and tcur.FLOCALEID={2}\r\n                                        where recard.ftaxprice<>0\r\n                                        group by tsd.FORDERID\r\n                                    ) td on (td.FORDERID=ts.FORDERID ) --如果一行订单对应的多张应收单，那么默认取第一行\r\n                                    when matched then update set ts.FCURRENCYID=td.fcurrencyid,ts.FMONEYTYPE=td.fcurrencyname,\r\n                                    ts.FRECUNITID=td.funitid,ts.fprice=td.fprice,ts.fcharge=td.fcharge ", this.tempTableName, this.flowTable, base.Context.UserLocale.LCID);
                list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            }
            DBUtils.ExecuteBatchWithTime(base.Context, list, 300);
        }
        public void GetSQLWhere()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine(" ");
            stringBuilder.AppendLine(" P.FENTRYID IS NOT NULL");
            stringBuilder.AppendLine(" AND V.FCANCELSTATUS ='A'");
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
            this.creatWhereFirst = stringBuilder;
        }
        public void CreatSQLWhereForTemp()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine(" WHERE  PD.FENTRYID IS NOT NULL ");
            if (this.filterShipment == 2)
            {
                stringBuilder.AppendLine(" AND PR.FBASEDELIQTY=PD.FBASEUNITQTY ");
            }
            if (this.filterShipment == 3)
            {
                stringBuilder.AppendLine(" AND (PR.FBASEDELIQTY<PD.FBASEUNITQTY OR PR.FBASEDELIQTY IS NULL OR PR.FBASEDELIQTY=0 ) ");
            }
            if (this.filterOutStock == 2)
            {
                stringBuilder.AppendLine(" AND PR.FBASESTOCKOUTQTY=PD.FBASEUNITQTY ");
            }
            if (this.filterOutStock == 3)
            {
                stringBuilder.AppendLine(" AND (PR.FBASESTOCKOUTQTY<PD.FBASEUNITQTY  OR PR.FBASESTOCKOUTQTY IS NULL OR PR.FBASESTOCKOUTQTY=0 ) ");
            }
            if (this.filterInvoece == 2)
            {
                stringBuilder.AppendLine(" AND T7.FIQTY=PD.FBASEUNITQTY ");
            }
            if (this.filterInvoece == 3)
            {
                stringBuilder.AppendLine(" AND (T7.FIQTY<PD.FBASEUNITQTY  OR T7.FIQTY IS NULL OR T7.FIQTY=0 )");
            }
            if (this.filterReturnS == 2)
            {
                stringBuilder.AppendLine(" AND PR.FBASERETURNQTY>0 ");
            }
            if (this.filterReturnS == 3)
            {
                stringBuilder.AppendLine(" AND (PR.FBASERETURNQTY=0 OR PR.FBASERETURNQTY IS NULL) ");
            }
            this.creatWhereSecond = stringBuilder;
            stringBuilder = new StringBuilder();
            if (this.filterReceipt == 2)
            {
                stringBuilder.AppendLine("  TSOF.FALLAMOUNT=T1.FICAMOUNT");
            }
            if (this.filterReceipt == 3)
            {
                stringBuilder.AppendLine(" TSOF.FALLAMOUNT>T1.FICAMOUNT OR T1.FICAMOUNT  IS NULL OR T1.FICAMOUNT =0 ");
            }
            this.creatWhereThrid = stringBuilder;
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
                "FSOAMOUNT",
                "FSAMOUNT",
                "FALREADYAMOUNT",
                "FNOTAMOUNT",
                "FRETURNPAMOUNT",
                "FRECAMOUNT",
                "FRECNAMOUNT",
                "FNVOECEAMOUNT",
                "FNNVOECEAMOUNT",
                "FADVANCEAMOUNT",
                "FRECEIPTAMOUNT",
                "FCHARGEOFFAMOUNT",
                "FNRECEIPTAMOUNT"
            };
        }
        private ReportTitles BuilderTitle(IRptParams filter)
        {
            ReportTitles reportTitles = new ReportTitles();
            DynamicObject customFilter = filter.FilterParameter.CustomFilter;
            if (customFilter != null)
            {
                if (this.filterOrgList.Trim().Length > 0)
                {
                    reportTitles.AddTitle("FSaleOrgListTitle", this.OrgNameList(this.filterOrgList));
                }
                else
                {
                    reportTitles.AddTitle("FSaleOrgListTitle", "");
                }
                string arg = string.Empty;
                string text = (this.GetDataByKey(customFilter, "SoFromDate") == string.Empty || Convert.ToDateTime(this.GetDataByKey(customFilter, "SoFromDate")) == DateTime.MinValue) ? "" : FieldFormatterUtil.GetDateFormatString(base.Context, Convert.ToDateTime(this.GetDataByKey(customFilter, "SoFromDate")));
                string text2 = (this.GetDataByKey(customFilter, "SoToDate") == string.Empty || Convert.ToDateTime(this.GetDataByKey(customFilter, "SoToDate")) == DateTime.MinValue) ? "" : FieldFormatterUtil.GetDateFormatString(base.Context, Convert.ToDateTime(this.GetDataByKey(customFilter, "SoToDate")));
                if (!text.IsNullOrEmptyOrWhiteSpace() || !text2.IsNullOrEmptyOrWhiteSpace())
                {
                    arg = ResManager.LoadKDString(" 至 ", "004102030003172", SubSystemType.SCM, new object[0]);
                }
                reportTitles.AddTitle("FDeliveryStartDay", string.Format("{0}{1}{2}", text, arg, text2));
                arg = string.Empty;
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
                if (this.GetDataByKey(customFilter, "SaleOFrom").ToString().Trim() != string.Empty || this.GetDataByKey(customFilter, "SaleOTo").Trim() != string.Empty)
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
                reportTitles.AddTitle("FIndentLimit", string.Format("{0}{1}{2}", this.GetDataByKey(customFilter, "SaleOFrom"), arg, this.GetDataByKey(customFilter, "SaleOTo")));
                arg = string.Empty;
                string arg14 = string.Empty;
                string arg15 = string.Empty;
                DynamicObject dynamicObject5 = customFilter["MaterialFrom"] as DynamicObject;
                string text3 = string.Empty;
                string arg16 = string.Empty;
                if (dynamicObject5 != null)
                {
                    text3 = this.GetDataByKey(dynamicObject5, "NUMBER");
                    arg16 = this.GetDataByKey(dynamicObject5, "NAME");
                }
                DynamicObject dynamicObject6 = customFilter["MaterialTo"] as DynamicObject;
                string text4 = string.Empty;
                string arg17 = string.Empty;
                if (dynamicObject6 != null)
                {
                    text4 = this.GetDataByKey(dynamicObject6, "NUMBER");
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
                if (text3 != string.Empty)
                {
                    arg14 = string.Format("{0}({1})", text3, arg16);
                }
                if (text4 != string.Empty)
                {
                    arg15 = string.Format("{0}({1})", text4, arg17);
                }
                reportTitles.AddTitle("FMaterialLimit", string.Format("{0}{1}{2}", arg14, arg, arg15));
                arg = string.Empty;
            }
            return reportTitles;
        }
        private void SetFilter(IRptParams filter)
        {
            object paramter = Kingdee.BOS.Contracts.ServiceFactory.GetService<ISystemParameterService>(base.Context).GetParamter(base.Context, -1L, -1L, "TM_SystemParameter", "ENABLEDTAXMIX", 0L);
            if (paramter != null)
            {
                Convert.ToBoolean(paramter);
            }
            DynamicObject customFilter = filter.FilterParameter.CustomFilter;
            this.filterMoneyType = ((this.GetBaseDataByKey(customFilter, "MoneyType", "Id").Trim() == "") ? 0 : Convert.ToInt32(this.GetBaseDataByKey(customFilter, "MoneyType", "Id")));
            this.filterSoDayFrom = ((this.GetDataByKey(customFilter, "SoFromDate") == string.Empty) ? DateTime.MinValue : Convert.ToDateTime(this.GetDataByKey(customFilter, "SoFromDate")));
            this.filterSoDayTo = ((this.GetDataByKey(customFilter, "SoToDate") == string.Empty) ? DateTime.MaxValue : Convert.ToDateTime(this.GetDataByKey(customFilter, "SoToDate")));
            this.filterDeDayFrom = ((this.GetDataByKey(customFilter, "DelliveryDateFrom") == string.Empty) ? DateTime.MinValue : Convert.ToDateTime(this.GetDataByKey(customFilter, "DelliveryDateFrom")));
            this.filterDeDayTo = ((this.GetDataByKey(customFilter, "DelliveryDateTo") == string.Empty) ? DateTime.MaxValue : Convert.ToDateTime(this.GetDataByKey(customFilter, "DelliveryDateTo")));
            this.filterSFrom = this.GetDataByKey(customFilter, "SaleOFrom").Trim();
            this.filterSTo = this.GetDataByKey(customFilter, "SaleOTo").Trim();
            DynamicObject dynamicObject = customFilter["MaterialFrom"] as DynamicObject;
            this.filterMaterialFrom = ((dynamicObject == null) ? "" : this.GetDataByKey(dynamicObject, "NUMBER"));
            DynamicObject dynamicObject2 = customFilter["MaterialTo"] as DynamicObject;
            this.filterMaterialTo = ((dynamicObject2 == null) ? "" : this.GetDataByKey(dynamicObject2, "NUMBER"));
            this.filterOrgList = this.GetDataByKey(customFilter, "SaleOrgList");
            DynamicObject dynamicObject3 = customFilter["CustomerFrom"] as DynamicObject;
            this.filterCustomerFrom = ((dynamicObject3 == null) ? "" : this.GetDataByKey(dynamicObject3, "NUMBER"));
            DynamicObject dynamicObject4 = customFilter["CustomerTo"] as DynamicObject;
            this.filterCustomerTo = ((dynamicObject4 == null) ? "" : this.GetDataByKey(dynamicObject4, "NUMBER"));
            DynamicObject dynamicObject5 = customFilter["SalesFrom"] as DynamicObject;
            this.filterSalesFrom = ((dynamicObject5 == null) ? "" : this.GetDataByKey(dynamicObject5, "NUMBER"));
            DynamicObject dynamicObject6 = customFilter["SalesTo"] as DynamicObject;
            this.filterSalesTo = ((dynamicObject6 == null) ? "" : this.GetDataByKey(dynamicObject6, "NUMBER"));
            this.bMergingSOHeader = Convert.ToBoolean(this.GetDataByKey(customFilter, "MergingSOHeader"));
            this.bIncludedUnfilledOrders = Convert.ToBoolean(this.GetDataByKey(customFilter, "IncludedUnfilledOrders"));
            this.isRecWithMat = Convert.ToBoolean(this.GetDataByKey(customFilter, "IsRecWithMat"));
            this.filterFormStatus = this.GetDataByKey(customFilter, "FormStatus");
            this.filterFCStatus = this.GetDataByKey(customFilter, "FormCloseStatus");
            this.filterBCStatus = this.GetDataByKey(customFilter, "BusCloseStatus");
            this.filterShipment = Convert.ToInt32(this.GetDataByKey(customFilter, "Shipment"));
            this.filterOutStock = Convert.ToInt32(this.GetDataByKey(customFilter, "OutStock"));
            this.filterInvoece = Convert.ToInt32(this.GetDataByKey(customFilter, "Incoice"));
            this.filterReceipt = Convert.ToInt32(this.GetDataByKey(customFilter, "Receipt"));
            this.filterReturnS = Convert.ToInt32(this.GetDataByKey(customFilter, "ReturnStock"));
            this.filterOrgList = this.GetDataByKey(customFilter, "SaleOrgList");
            this.priceFrom = ((customFilter["PriceFrom"] != null) ? Convert.ToString(customFilter["PriceFrom"]) : "SALORDERBILL");
            this.superioreFilter = filter.FilterParameter.FilterString.ToString().Trim();
        }
        protected string GetDataByKey(DynamicObject dy, string key)
        {
            if (dy != null && dy[key] != null && !string.IsNullOrWhiteSpace(dy[key].ToString()))
            {
                return dy[key].ToString();
            }
            return string.Empty;
        }
        private decimal DataTr(string Data)
        {
            if (Data.Trim() == string.Empty)
            {
                return 0m;
            }
            return Convert.ToDecimal(Data);
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
        private string OrgNameList(string idList)
        {
            string text = string.Empty;
            if (idList.Trim().Length > 0)
            {
                IQueryService service = Kingdee.BOS.Contracts.ServiceFactory.GetService<IQueryService>(base.Context);
                QueryBuilderParemeter para = new QueryBuilderParemeter
                {
                    FormId = "ORG_Organizations",
                    SelectItems = SelectorItemInfo.CreateItems("FNAME,FORGID"),
                    FilterClauseWihtKey = string.Format(" FORGID IN ({0}) AND FLOCALEID={1}", idList, base.Context.UserLocale.LCID.ToString())
                };
                DynamicObjectCollection dynamicObjectCollection = service.GetDynamicObjectCollection(base.Context, para, null);
                foreach (DynamicObject current in dynamicObjectCollection)
                {
                    text = text + Convert.ToString(current["FNAME"]) + ";";
                }
                if (text.Length > 0)
                {
                    text = text.Substring(0, text.Length - 1);
                }
            }
            return text;
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
            this.SetFieldDecimal("FSOQTY", "FPRECISION");
            this.SetFieldDecimal("FORDERCHARGEQTY", "FPRECISION");
            this.SetFieldDecimal("FSOAMOUNT", "FAMOUNTDIGITS");
            this.SetFieldDecimal("FSQTY", "FPRECISION");
            this.SetFieldDecimal("FDELICHARGEQTY", "FPRECISION");
            this.SetFieldDecimal("FSAMOUNT", "FAMOUNTDIGITS");
            this.SetFieldDecimal("FALREADYQTY", "FPRECISION");
            this.SetFieldDecimal("FHAVEOUTCHARGEQTY", "FPRECISION");
            this.SetFieldDecimal("FALREADYAMOUNT", "FAMOUNTDIGITS");
            this.SetFieldDecimal("FNOTQTY", "FPRECISION");
            this.SetFieldDecimal("FNOTOUTCHARGEQTY", "FPRECISION");
            this.SetFieldDecimal("FNOTAMOUNT", "FAMOUNTDIGITS");
            this.SetFieldDecimal("FRETURNPQTY", "FPRECISION");
            this.SetFieldDecimal("FRETURNPCHARGEQTY", "FPRECISION");
            this.SetFieldDecimal("FRETURNPAMOUNT", "FAMOUNTDIGITS");
            this.SetFieldDecimal("FRETURNSQTY", "FPRECISION");
            this.SetFieldDecimal("FRETURNCHARGEQTY", "FPRECISION");
            this.SetFieldDecimal("FRETURNSAMOUNT", "FAMOUNTDIGITS");
            this.SetFieldDecimal("FRECECHARGEQTY", "FPRECISION");
            this.SetFieldDecimal("FRECNCHARGEQTY", "FPRECISION");
            this.SetFieldDecimal("FINVOECEQTY", "FPRECISION");
            this.SetFieldDecimal("FINVOECECHARGEQTY", "FPRECISION");
            this.SetFieldDecimal("FNVOECEAMOUNT", "FAMOUNTDIGITS");
            this.SetFieldDecimal("FNINVOECEQTY", "FPRECISION");
            this.SetFieldDecimal("FNINVOECECHARGEQTY", "FPRECISION");
            this.SetFieldDecimal("FNNVOECEAMOUNT", "FAMOUNTDIGITS");
            this.SetFieldDecimal("FRECEIPTAMOUNT", "FAMOUNTDIGITS");
            this.SetFieldDecimal("FNRECEIPTAMOUNT", "FAMOUNTDIGITS");
            this.SetFieldDecimal("FADVANCEAMOUNT", "FAMOUNTDIGITS");
            this.SetFieldDecimal("FCHARGEOFFAMOUNT", "FAMOUNTDIGITS");
            this.SetFieldDecimal("FRECQTY", "FPRECISION");
            this.SetFieldDecimal("FRECECHARGEQTY", "FPRECISION");
            this.SetFieldDecimal("FRECAMOUNT", "FAMOUNTDIGITS");
            this.SetFieldDecimal("FRECNAMOUNT", "FAMOUNTDIGITS");
            this.SetFieldDecimal("FWRITEOFFAMOUNT", "FAMOUNTDIGITS");
            base.ReportProperty.DecimalControlFieldList = this.lstDcf;
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
                result = dynamicObjectCollection[0][selectColumn].ToString().Trim();
            }
            return result;
        }
        protected bool IsCanViewAmount(IRptParams filter)
        {
            IPermissionService permissionService = Kingdee.BOS.Contracts.ServiceFactory.GetPermissionService(base.Context);
            PermissionAuthResult permissionAuthResult = permissionService.FuncPermissionAuth(base.Context, new BusinessObject
            {
                Id = "SAL_CollectReport"
            }, "1ff7fbd3148440809820b8f039a228a9");
            Kingdee.BOS.Contracts.ServiceFactory.CloseService(permissionService);
            if (!permissionAuthResult.Passed)
            {
                return false;
            }
            string dataByKey = this.GetDataByKey(filter.FilterParameter.CustomFilter, "SaleOrgList");
            if (dataByKey.IsNullOrEmptyOrWhiteSpace())
            {
                return true;
            }
            string[] array = dataByKey.Split(new char[]
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
        public void SetRptBosTable(string tableName)
        {
            this.KSQL_SEQ = string.Format(this.KSQL_SEQ, "FSALEORGID, FID,FBILLNO,FDATE ASC ");
            List<SqlObject> list = new List<SqlObject>();
            StringBuilder stringBuilder = new StringBuilder();
            this.MergeOrderBillType(this.tempTableName, list);
            stringBuilder.AppendLine(" SELECT ");
            stringBuilder.AppendLine(this.GetFiledData());
            stringBuilder.AppendLine(string.Format(" , {0} INTO {1} FROM {2} ", this.KSQL_SEQ, tableName, this.tempTableName));
            stringBuilder.AppendLine("  ");
            list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
            bool flag = !string.IsNullOrWhiteSpace(this.priceFrom) && this.priceFrom.Equals("RECEIVEBILL", StringComparison.CurrentCultureIgnoreCase);
            if (flag)
            {
                list.Add(new SqlObject(this.GetMergeSQLForARSrc(tableName), new List<SqlParam>()));
            }
            stringBuilder.AppendLine(string.Format(" TRUNCATE TABLE {0} ", this.tempTableName));
            list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
            stringBuilder.AppendLine(string.Format(" TRUNCATE TABLE {0} ", this.rptFilterTable));
            list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
            stringBuilder.AppendLine(string.Format(" TRUNCATE TABLE {0} ", this.flowTable));
            list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
            stringBuilder.AppendLine(string.Format(" TRUNCATE TABLE {0} ", this.receivableHelpTable));
            list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
            stringBuilder.AppendLine(string.Format(" TRUNCATE TABLE {0} ", this.receivableTable));
            list.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
            stringBuilder.Clear();
            DBUtils.ExecuteBatchWithTime(base.Context, list, 300);
        }
        private void MergeOrderBillType(string tempTableName, List<SqlObject> sqlObjList)
        {
            string sql = string.Format("/*dialect*/\r\nMERGE INTO {0} T USING(\r\nSELECT SO.FID,BT.FNAME FBILLTYPENAME\r\n FROM T_SAL_ORDER  SO\r\n LEFT JOIN T_BAS_BILLTYPE_L BT ON SO.FBILLTYPEID=BT.FBILLTYPEID AND BT.FLOCALEID={1}\r\n) BT ON (BT.FID=T.FID)\r\nWHEN MATCHED THEN UPDATE SET T.FBILLTYPE=BT.FBILLTYPENAME {2}", tempTableName, base.Context.UserLocale.LCID, (base.Context.DatabaseType == DatabaseType.MS_SQL_Server) ? ";" : "");
            sqlObjList.Add(new SqlObject(sql, new List<SqlParam>()));
        }
        private void MergePriceQTY(List<SqlObject> lstSql)
        {
            StringBuilder stringBuilder = new StringBuilder();
            IUnitConvertService service = ServiceHelper.GetService<IUnitConvertService>();
            string strSQL = string.Format("SELECT * FROM {0} ST WHERE (ST.FOUTPRICEUNITID <> 0 AND ST.FPRICEUNITID <> ST.FOUTPRICEUNITID) \r\n                                                            OR (ST.FDELIPRICEUNITID <> 0 AND ST.FPRICEUNITID <> ST.FDELIPRICEUNITID) \r\n                                                            OR (ST.FRETURNPRICEUNITID <> 0 AND ST.FPRICEUNITID <> ST.FRETURNPRICEUNITID)\r\n                                                            OR (ST.FRETNOTICEPRICEUNITID<> 0 AND ST.FPRICEUNITID <> ST.FRETNOTICEPRICEUNITID)\r\n                                                            OR (ST.FRECEPRICEUNITID <> 0 AND ST.FPRICEUNITID <> ST.FRECEPRICEUNITID)", this.tempTableName);
            DynamicObjectCollection dynamicObjectCollection = DBUtils.ExecuteDynamicObject(base.Context, strSQL, null, null, CommandType.Text, new SqlParam[0]);
            foreach (DynamicObject current in dynamicObjectCollection)
            {
                int num = Convert.ToInt32(current["FORDERID"]);
                int num2 = Convert.ToInt32(current["FROWNO"]);
                int num3 = Convert.ToInt32(current["FPRICEUNITID"]);
                int num4 = Convert.ToInt32(current["FOUTPRICEUNITID"]);
                int num5 = Convert.ToInt32(current["FDELIPRICEUNITID"]);
                int num6 = Convert.ToInt32(current["FRETNOTICEPRICEUNITID"]);
                int num7 = Convert.ToInt32(current["FRETURNPRICEUNITID"]);
                int num8 = Convert.ToInt32(current["FRECEPRICEUNITID"]);
                if (num4 != 0 && num3 != num4)
                {
                    string format = "MERGE INTO {0} ST USING  \r\n                        (\r\n\t                        SELECT IT.FORDERID,IT.FHAVEOUTCHARGEQTY*IT.RATE AS  FHAVEOUTCHARGEQTY\r\n                            FROM ({1}) IT  WHERE IT.FORDERID= {2} AND IT.FROWNO = {3}\r\n                        )TT ON (ST.FORDERID = TT.FORDERID)\r\n                        WHEN MATCHED THEN UPDATE SET ST.FHAVEOUTCHARGEQTY = TT.FHAVEOUTCHARGEQTY WHERE ST.FROWNO={3}";
                    UnitConvertParam unitConvertParam = new UnitConvertParam(true)
                    {
                        TableName = this.tempTableName,
                        TableNameAlias = "T1",
                        SrcUnitFieldName = "FOUTPRICEUNITID",
                        DstUntiFieldName = "FPRICEUNITID",
                        MaterialFieldName = "FMATERIALID",
                        TablePrimaryKey = "FID"
                    };
                    unitConvertParam.OtherFieldName = new List<string>
                    {
                        "FHAVEOUTCHARGEQTY",
                        "FORDERID",
                        "FROWNO"
                    };
                    stringBuilder.AppendLine(string.Format(format, new object[]
                    {
                        this.tempTableName,
                        service.GetUnitConvertSql(unitConvertParam),
                        num,
                        num2
                    }));
                    lstSql.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
                    stringBuilder.Clear();
                }
                if (num6 != 0 && num3 != num6)
                {
                    string format2 = "MERGE INTO {0} ST USING  \r\n                        (\r\n\t                        SELECT IT.FORDERID,IT.FRETURNPCHARGEQTY*IT.RATE AS  FRETURNPCHARGEQTY\r\n                            FROM ({1}) IT WHERE IT.FORDERID= {2} AND IT.FROWNO = {3}\r\n                        )TT ON (ST.FORDERID = TT.FORDERID)\r\n                        WHEN MATCHED THEN UPDATE SET ST.FRETURNPCHARGEQTY = TT.FRETURNPCHARGEQTY WHERE ST.FROWNO={3}";
                    UnitConvertParam unitConvertParam2 = new UnitConvertParam(true)
                    {
                        TableName = this.tempTableName,
                        TableNameAlias = "T1",
                        SrcUnitFieldName = "FRETNOTICEPRICEUNITID",
                        DstUntiFieldName = "FPRICEUNITID",
                        MaterialFieldName = "FMATERIALID",
                        TablePrimaryKey = "FID"
                    };
                    unitConvertParam2.OtherFieldName = new List<string>
                    {
                        "FRETURNPCHARGEQTY",
                        "FORDERID",
                        "FROWNO"
                    };
                    stringBuilder.AppendLine(string.Format(format2, new object[]
                    {
                        this.tempTableName,
                        service.GetUnitConvertSql(unitConvertParam2),
                        num,
                        num2
                    }));
                    lstSql.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
                    stringBuilder.Clear();
                }
                if (num5 != 0 && num3 != num5)
                {
                    string format3 = "MERGE INTO {0} ST USING  \r\n                        (\r\n\t                        SELECT IT.FORDERID,IT.FDELICHARGEQTY*IT.RATE AS  FDELICHARGEQTY\r\n                            FROM ({1}) IT WHERE IT.FORDERID= {2} AND IT.FROWNO = {3}\r\n                        )TT ON (ST.FORDERID = TT.FORDERID)\r\n                        WHEN MATCHED THEN UPDATE SET ST.FDELICHARGEQTY = TT.FDELICHARGEQTY WHERE ST.FROWNO={3}";
                    UnitConvertParam unitConvertParam3 = new UnitConvertParam(true)
                    {
                        TableName = this.tempTableName,
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
                        "FROWNO"
                    };
                    stringBuilder.AppendLine(string.Format(format3, new object[]
                    {
                        this.tempTableName,
                        service.GetUnitConvertSql(unitConvertParam3),
                        num,
                        num2
                    }));
                    lstSql.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
                    stringBuilder.Clear();
                }
                if (num7 != 0 && num3 != num7)
                {
                    string format4 = "MERGE INTO {0} ST USING  \r\n                        (\r\n\t                        SELECT IT.FORDERID,IT.FRETURNCHARGEQTY*IT.RATE AS  FRETURNCHARGEQTY\r\n                            FROM ({1}) IT WHERE IT.FORDERID= {2} AND IT.FROWNO = {3}\r\n                        )TT ON (ST.FORDERID = TT.FORDERID)\r\n                        WHEN MATCHED THEN UPDATE SET ST.FRETURNCHARGEQTY = TT.FRETURNCHARGEQTY WHERE ST.FROWNO={3}";
                    UnitConvertParam unitConvertParam4 = new UnitConvertParam(true)
                    {
                        TableName = this.tempTableName,
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
                        "FROWNO"
                    };
                    stringBuilder.AppendLine(string.Format(format4, new object[]
                    {
                        this.tempTableName,
                        service.GetUnitConvertSql(unitConvertParam4),
                        num,
                        num2
                    }));
                    lstSql.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
                    stringBuilder.Clear();
                }
                if (num8 != 0 && num3 != num8)
                {
                    string format5 = "MERGE INTO {0} ST USING  \r\n                        (\r\n\t                        SELECT IT.FORDERID,IT.FRECECHARGEQTY*IT.RATE AS  FRECECHARGEQTY\r\n                            FROM ({1}) IT WHERE IT.FORDERID= {2} AND IT.FROWNO = {3}\r\n                        )TT ON (ST.FORDERID = TT.FORDERID)\r\n                        WHEN MATCHED THEN UPDATE SET ST.FRECECHARGEQTY = TT.FRECECHARGEQTY WHERE ST.FROWNO={3}";
                    UnitConvertParam unitConvertParam5 = new UnitConvertParam(true)
                    {
                        TableName = this.tempTableName,
                        TableNameAlias = "T1",
                        SrcUnitFieldName = "FRECEPRICEUNITID",
                        DstUntiFieldName = "FPRICEUNITID",
                        MaterialFieldName = "FMATERIALID",
                        TablePrimaryKey = "FID"
                    };
                    unitConvertParam5.OtherFieldName = new List<string>
                    {
                        "FRECECHARGEQTY",
                        "FORDERID",
                        "FROWNO"
                    };
                    stringBuilder.AppendLine(string.Format(format5, new object[]
                    {
                        this.tempTableName,
                        service.GetUnitConvertSql(unitConvertParam5),
                        num,
                        num2
                    }));
                    lstSql.Add(new SqlObject(stringBuilder.ToString(), new List<SqlParam>()));
                    stringBuilder.Clear();
                }
            }
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
            SalRptCommon.CreateAdvanceTable(base.Context, this.rptFilterTable, list);
            this.deleteTables.Add(this.rptFilterTable);
            this.tempTableName = SalRptCommon.AddTempTable(base.Context);
            this.CreateTmpRptTable(this.tempTableName, list);
            this.deleteTables.Add(this.tempTableName);
            this.flowTable = SalRptCommon.AddTempTable(base.Context);
            SalRptCommon.CreateFlowTable(base.Context, this.flowTable, list);
            this.deleteTables.Add(this.flowTable);
            this.noCalTable = SalRptCommon.AddTempTable(base.Context);
            this.CreateDetailTable(this.noCalTable, list);
            this.deleteTables.Add(this.noCalTable);
            this.receivableHelpTable = SalRptCommon.AddTempTable(base.Context);
            this.deleteTables.Add(this.receivableHelpTable);
            SalRptCommon.CreateReceivableHelpTable(this.receivableHelpTable, list);
            this.receivableTable = SalRptCommon.AddTempTable(base.Context);
            this.deleteTables.Add(this.receivableTable);
            SalRptCommon.CreateReceivableTable(this.receivableTable, list);
            this.flowDataTable = SalRptCommon.AddTempTable(base.Context);
            using (KDTransactionScope kDTransactionScope = new KDTransactionScope(TransactionScopeOption.RequiresNew))
            {
                DBUtils.ExecuteBatchWithTime(base.Context, list, 100);
                kDTransactionScope.Complete();
            }
        }
        public string GetFiledData()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("  FPRECISION , FPRICEDIGITS, FAMOUNTDIGITS, ");
            stringBuilder.AppendLine("  FSALEORGID ,FSALEORGNUMBER,FSALEORGNAME,FID, FBILLNO,FBILLTYPE, FDATE, FORDERID, ");
            stringBuilder.AppendLine("  FSALEDEPTID,FSALEDEPT,FSALEGROUPID,FSALEGROUP,FSALERID,FSALERNUMBER,FSALES,");
            stringBuilder.AppendLine("  FCUSTID,FCUSTOMERNUMBER,FCUSTOMERNAME,FDELIVERYDAY,");
            stringBuilder.AppendLine("  FMATERIALID ,FMATERIALNUMBER, FMATERIALNAME ,FMATERIALGROUP,FMATERIALGROUPNAME ,");
            stringBuilder.AppendLine("  FMAPID ,FMAPNUMBER, FMAPNAME ,");
            stringBuilder.AppendLine("  FSPECIFICATION,FAuxPropId,FAuxProp,");
            stringBuilder.AppendLine("  FUNITID,FUNITNAME ,FPRICEUNITID,FPRICEUNITNAME, FCURRENCYID,FMONEYTYPE, FPRICE ,FCHARGE,");
            stringBuilder.AppendLine("  FSOQTY ,FORDERCHARGEQTY, FSOAMOUNT , FSQTY , FDELICHARGEQTY,FSAMOUNT , FALREADYQTY , FHAVEOUTCHARGEQTY,FALREADYAMOUNT ,");
            stringBuilder.AppendLine("  FNOTQTY ,FNOTOUTCHARGEQTY, FNOTAMOUNT , FRETURNPQTY , FRETURNPCHARGEQTY ,FRETURNPAMOUNT , FRETURNSQTY ,FRETURNCHARGEQTY ,FRETURNSAMOUNT , ");
            stringBuilder.AppendLine("  FRECQTY , FRECECHARGEQTY ,FRECAMOUNT , FRECNQTY ,FRECNCHARGEQTY ,  FRECNAMOUNT , FWRITEOFFAMOUNT ,FSETACCOUNTTYPE,");
            stringBuilder.AppendLine("  FINVOECEQTY , FINVOECECHARGEQTY ,FNVOECEAMOUNT , FNINVOECEQTY ,FNINVOECECHARGEQTY , FNNVOECEAMOUNT , ");
            stringBuilder.AppendLine("  FRECEIPTAMOUNT , FNRECEIPTAMOUNT,FCHARGEOFFAMOUNT,FADVANCEAMOUNT, FROWNO, ");
            stringBuilder.AppendLine("  FBASEUNITID_ID,FBASEUNITID,FORDERBASEQTY,FBASEPRICE,FDELIVERYNOTICEQTY,FDELIVERYQTY,FUNDELIVERYQTY,FRETURNNOTICEQTY,FRETURNBASEQTY,FRECEIVEQTY,FUNCONFIRMQTY,FBASEINVOICEQTY , ");
            stringBuilder.AppendLine("  FCLOSESTATUS,FMRPCLOSESTATUS,FMRPTERMINATESTATUS,FRECUNITID");
            return stringBuilder.ToString();
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
            this.hidenColumns.Add("FCHARGE_LC", SqlStorageType.SqlDecimal);
            this.hidenColumns.Add("FBASEPRICE_LC", SqlStorageType.SqlDecimal);
            this.hidenColumns.Add("FSOAMOUNT_LC", SqlStorageType.SqlDecimal);
            this.hidenColumns.Add("FSAMOUNT_LC", SqlStorageType.SqlDecimal);
            this.hidenColumns.Add("FALREADYAMOUNT_LC", SqlStorageType.SqlDecimal);
            this.hidenColumns.Add("FNOTAMOUNT_LC", SqlStorageType.SqlDecimal);
            this.hidenColumns.Add("FRETURNPAMOUNT_LC", SqlStorageType.SqlDecimal);
            this.hidenColumns.Add("FRETURNSAMOUNT_LC", SqlStorageType.SqlDecimal);
            this.hidenColumns.Add("FRECAMOUNT_LC", SqlStorageType.SqlDecimal);
            this.hidenColumns.Add("FWRITEOFFAMOUNT_LC", SqlStorageType.SqlDecimal);
            this.hidenColumns.Add("FRECNAMOUNT_LC", SqlStorageType.SqlDecimal);
            this.hidenColumns.Add("FNVOECEAMOUNT_LC", SqlStorageType.SqlDecimal);
            this.hidenColumns.Add("FNNVOECEAMOUNT_LC", SqlStorageType.SqlDecimal);
            this.hidenColumns.Add("FADVANCEAMOUNT_LC", SqlStorageType.SqlDecimal);
            this.hidenColumns.Add("FRECEIPTAMOUNT_LC", SqlStorageType.SqlDecimal);
            this.hidenColumns.Add("FCHARGEOFFAMOUNT_LC", SqlStorageType.SqlDecimal);
            this.hidenColumns.Add("FNRECEIPTAMOUNT_LC", SqlStorageType.SqlDecimal);
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
            string format = "MERGE INTO {0} ST USING  \r\n                        (\r\n\t                        SELECT IT.FIDENTITYID,ROUND(IT.FPRICE,IT.FPRECISION)*IT.RATE*ISNULL(IT.FSOQTY,0)  AS  FSOAMOUNT,\r\n                            ROUND(IT.FPRICE,IT.FPRECISION)*IT.RATE*ISNULL(IT.FSQTY,0)  AS  FSAMOUNT,\r\n                            ROUND(IT.FPRICE,IT.FPRECISION)*IT.RATE*ISNULL(IT.FALREADYQTY,0)  AS  FALREADYAMOUNT,\r\n                            ROUND(IT.FPRICE,IT.FPRECISION)*IT.RATE*ISNULL(IT.FNOTQTY,0)  AS  FNOTAMOUNT,\r\n                            ROUND(IT.FPRICE,IT.FPRECISION)*IT.RATE*ISNULL(IT.FRETURNPQTY,0)  AS  FRETURNPAMOUNT,\r\n                            ROUND(IT.FPRICE,IT.FPRECISION)*IT.RATE*ISNULL(IT.FRETURNSQTY,0)  AS  FRETURNSAMOUNT\r\n                            FROM ({1}) IT  \r\n                        )TT ON (ST.FIDENTITYID = TT.FIDENTITYID)\r\n                        WHEN MATCHED THEN UPDATE SET ST.FSOAMOUNT = TT.FSOAMOUNT,ST.FSAMOUNT = TT.FSAMOUNT,\r\n                            ST.FALREADYAMOUNT = TT.FALREADYAMOUNT, ST.FNOTAMOUNT = TT.FNOTAMOUNT,ST.FRETURNPAMOUNT = TT.FRETURNPAMOUNT,ST.FRETURNSAMOUNT = TT.FRETURNSAMOUNT ";
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
                    "FSOQTY",
                    "FSQTY",
                    "FALREADYQTY",
                    "FNOTQTY",
                    "FRETURNPQTY",
                    "FRETURNSQTY"
                }
            }));
        }
    }
}
