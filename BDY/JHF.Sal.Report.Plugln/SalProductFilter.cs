using Kingdee.BOS;
using Kingdee.BOS.Contracts;
using Kingdee.BOS.Core.CommonFilter;
using Kingdee.BOS.Core.CommonFilter.PlugIn;
using Kingdee.BOS.Core.DynamicForm;
using Kingdee.BOS.Core.DynamicForm.PlugIn.Args;
using Kingdee.BOS.Core.DynamicForm.PlugIn.ControlModel;
using Kingdee.BOS.Core.Enums;
using Kingdee.BOS.Core.List;
using Kingdee.BOS.Core.Metadata;
using Kingdee.BOS.Core.Permission;
using Kingdee.BOS.Core.SqlBuilder;
using Kingdee.BOS.Orm.DataEntity;
using Kingdee.BOS.Resource;
using Kingdee.BOS.ServiceHelper;
using Kingdee.BOS.Util;
using System;
using System.Collections.Generic;
using System.Text;
namespace JHF.Sal.Report.Plugln
{
   public  class SalProductFilter : AbstractCommonFilterPlugIn
    {
        private List<long> lstSalOrg = new List<long>();
        private string selOrgId = string.Empty;
        public override void OnLoad(EventArgs e)
        {
            base.OnLoad(e);
            this.View.StyleManager.SetVisible("FTab_P21", null, false);
            this.SetPriceFrom();
        }
        public override void TreeNodeClick(TreeNodeArgs e)
        {
            base.TreeNodeClick(e);
            this.InitSalOrgId();
            this.SetPriceFrom();
        }
        public override void AfterBindData(EventArgs e)
        {
            base.AfterBindData(e);
            if (!base.Context.IsMultiOrg)
            {
                this.View.StyleManager.SetEnabled("FSaleOrgList", null, false);
            }
        }
        public override void DataChanged(DataChangedEventArgs e)
        {
            string a;
            if ((a = e.Field.Key.ToUpperInvariant()) != null && a == "FSALEORGLIST")
            {
                this.selOrgId = e.NewValue.ToString();
            }
            base.DataChanged(e);
        }
        public override void ButtonClick(ButtonClickEventArgs e)
        {
            base.ButtonClick(e);
            string a;
            if ((a = e.Key.ToUpperInvariant()) != null)
            {
                if (!(a == "FBTNOK"))
                {
                    return;
                }
                this.SetPriceFrom();
                this.CheckValit(e);
            }
        }
        public override void BeforeF7Select(BeforeF7SelectEventArgs e)
        {
            string key;
            switch (key = e.FieldKey.ToUpperInvariant())
            {
                case "FSALEOFROM":
                    this.GetSaleOrderListData(e, "FSaleOFrom", "SAL_SaleOrder", "FBILLNO");
                    break;
                case "FSALEOTO":
                    this.GetSaleOrderListData(e, "FSaleOTo", "SAL_SaleOrder", "FBILLNO");
                    break;
                case "FCUSTOMERFROM":
                case "FCUSTOMERTO":
                case "FMATERIALFROM":
                case "FMATERIALTO":
                case "FSALESFROM":
                case "FSALESTO":
                    {
                        string text = this.View.Model.GetValue("FSaleOrgList").ToString();
                        if (text.Length == 0)
                        {
                            this.View.ShowMessage(ResManager.LoadKDString("请先选择销售组织！", "004104030002440", SubSystemType.SCM, new object[0]), MessageBoxType.Notice);
                            e.Cancel = true;
                            return;
                        }
                        ListShowParameter listShowParameter = e.DynamicFormShowParameter as ListShowParameter;
                        listShowParameter.MutilListUseOrgId = text;
                        break;
                    }
            }
            base.BeforeF7Select(e);
        }
        public override void BeforeSetItemValueByNumber(BeforeSetItemValueByNumberArgs e)
        {
            string text = "";
            if (this.GetFieldFilter(e.BaseDataFieldKey, out text))
            {
                if (string.IsNullOrEmpty(e.Filter))
                {
                    e.Filter += text;
                    return;
                }
                e.Filter = e.Filter + " AND " + text;
            }
        }
        private void SetPriceFrom()
        {
            object value = this.View.Model.GetValue("FPriceFrom");
            string value2 = (value != null) ? Convert.ToString(value) : string.Empty;
            if (string.IsNullOrWhiteSpace(value2))
            {
                this.View.Model.SetValue("FPriceFrom", "SALORDERBILL");
            }
        }
        private bool GetFieldFilter(string fieldKey, out string filter)
        {
            filter = "";
            string text = Convert.ToString(this.View.Model.GetValue("FSaleOrgList"));
            if (string.IsNullOrWhiteSpace(fieldKey) || string.IsNullOrWhiteSpace(text))
            {
                return false;
            }
            string key;
            switch (key = fieldKey.ToUpperInvariant())
            {
                case "FCUSTOMERFROM":
                case "FCUSTOMERTO":
                case "FMATERIALFROM":
                case "FMATERIALTO":
                    filter = " FUseOrgId in ( " + text + ") ";
                    break;
                case "FSALESFROM":
                case "FSALESTO":
                    filter = " FBizOrgId in ( " + text + ") ";
                    break;
            }
            return !string.IsNullOrWhiteSpace(filter);
        }
        public override void BeforeFilterGridF7Select(BeforeFilterGridF7SelectEventArgs e)
        {
            string a;
            if ((a = e.Key.ToUpperInvariant()) != null && a == "FBILLTYPENAME")
            {
                string text = string.Format(" FBILLFORMID='SAL_SaleOrder'", new object[0]);
                if (string.IsNullOrEmpty(e.ListFilterParameter.Filter))
                {
                    e.ListFilterParameter.Filter = text;
                }
                else
                {
                    IRegularFilterParameter expr_53 = e.ListFilterParameter;
                    expr_53.Filter = expr_53.Filter + " AND " + text;
                }
            }
            e.IsShowUsed = false;
        }
        private void InitSalOrgId()
        {
            if (this.View.ParentFormView != null)
            {
                this.lstSalOrg = this.GetPermissionOrg(this.View.ParentFormView.BillBusinessInfo.GetForm().Id);
            }
            List<EnumItem> organization = this.GetOrganization(this.View.Context);
            ComboFieldEditor fieldEditor = this.View.GetFieldEditor<ComboFieldEditor>("FSaleOrgList", 0);
            fieldEditor.SetComboItems(organization);
            object value = this.Model.GetValue("FSaleOrgList");
            if (value.IsNullOrEmpty() && base.Context.CurrentOrganizationInfo.FunctionIds.Contains(101L))
            {
                this.Model.SetValue("FSaleOrgList", base.Context.CurrentOrganizationInfo.ID);
                this.selOrgId = base.Context.CurrentOrganizationInfo.ID.ToString();
            }
            this.View.Model.GetValue("FMoneyType");
            object paramter = SystemParameterServiceHelper.GetParamter(base.Context, -1L, -1L, "TM_SystemParameter", "ENABLEDTAXMIX", 0L);
            if (paramter == null)
            {
                return;
            }
            Convert.ToBoolean(paramter);
        }
        private List<long> GetPermissionOrg(string formId)
        {
            BusinessObject bizObject = new BusinessObject
            {
                Id = formId,
                PermissionControl = this.View.ParentFormView.BillBusinessInfo.GetForm().SupportPermissionControl,
                SubSystemId = this.View.ParentFormView.Model.SubSytemId
            };
            return PermissionServiceHelper.GetPermissionOrg(base.Context, bizObject, "6e44119a58cb4a8e86f6c385e14a17ad");
        }
        private void CheckValit(ButtonClickEventArgs e)
        {
            StringBuilder stringBuilder = new StringBuilder();
            DateTime t = DateTime.Now;
            if (this.View.Model.GetValue("FSoFromDate") != null && this.View.Model.GetValue("FSoFromDate").ToString().Trim().Length > 0)
            {
                t = Convert.ToDateTime(this.View.Model.GetValue("FSoFromDate"));
            }
            DateTime t2 = DateTime.Now;
            if (this.View.Model.GetValue("FSoToDate") != null && this.View.Model.GetValue("FSoToDate").ToString().Trim().Length > 0)
            {
                t2 = Convert.ToDateTime(this.View.Model.GetValue("FSoToDate"));
            }
            DateTime t3 = DateTime.Now;
            if (this.View.Model.GetValue("FDelliveryDateFrom") != null && this.View.Model.GetValue("FDelliveryDateFrom").ToString().Trim().Length > 0)
            {
                t3 = Convert.ToDateTime(this.View.Model.GetValue("FDelliveryDateFrom"));
            }
            DateTime t4 = DateTime.Now;
            if (this.View.Model.GetValue("FDelliveryDateTo") != null && this.View.Model.GetValue("FDelliveryDateTo").ToString().Trim().Length > 0)
            {
                t4 = Convert.ToDateTime(this.View.Model.GetValue("FDelliveryDateTo"));
            }
            if (DateTime.Compare(t, t2) > 0)
            {
                stringBuilder.AppendLine(ResManager.LoadKDString("销售订单开始日期大于结束日期!", "004104030002443", SubSystemType.SCM, new object[0]));
            }
            if (DateTime.Compare(t3, t4) > 0)
            {
                stringBuilder.AppendLine(ResManager.LoadKDString("交货起始日期大于结束日期", "004104030002446", SubSystemType.SCM, new object[0]));
            }
            if (stringBuilder.Length > 0)
            {
                e.Cancel = true;
                this.View.ShowErrMessage(stringBuilder.ToString(), ResManager.LoadKDString("过滤条件格式错误！", "004104030002452", SubSystemType.SCM, new object[0]), MessageBoxType.Advise);
            }
        }
        protected List<EnumItem> GetOrganization(Context ctx)
        {
            List<EnumItem> list = new List<EnumItem>();
            List<SelectorItemInfo> list2 = new List<SelectorItemInfo>();
            list2.Add(new SelectorItemInfo("FORGID"));
            list2.Add(new SelectorItemInfo("FNUMBER"));
            list2.Add(new SelectorItemInfo("FNAME"));
            string text = this.GetInFilter("FORGID", this.lstSalOrg);
            text += string.Format(" AND FORGFUNCTIONS LIKE '%{0}%' ", 101L.ToString());
            QueryBuilderParemeter para = new QueryBuilderParemeter
            {
                FormId = "ORG_Organizations",
                SelectItems = list2,
                FilterClauseWihtKey = text
            };
            DynamicObjectCollection dynamicObjectCollection = QueryServiceHelper.GetDynamicObjectCollection(base.Context, para, null);
            foreach (DynamicObject current in dynamicObjectCollection)
            {
                list.Add(new EnumItem(new DynamicObject(EnumItem.EnumItemType))
                {
                    EnumId = current["FORGID"].ToString(),
                    Value = current["FORGID"].ToString(),
                    Caption = new LocaleValue(Convert.ToString(current["FName"]), base.Context.UserLocale.LCID)
                });
            }
            return list;
        }
        private string GetInFilter(string key, List<long> valList)
        {
            if (valList == null || valList.IsEmpty<long>())
            {
                return string.Format("{0} = -1 ", key);
            }
            return string.Format("{0} in ({1})", key, string.Join<long>(",", valList));
        }
        private void GetSaleOrderListData(BeforeF7SelectEventArgs e, string key_Field, string FormId, string key_Item)
        {
            if (FormId == null || FormId.ToString().Length == 0)
            {
                return;
            }
            ListShowParameter listShowParameter = new ListShowParameter();
            ListRegularFilterParameter listRegularFilterParameter = new ListRegularFilterParameter();
            listRegularFilterParameter.Filter = string.Format(" FSALEORGID IN ({0})", this.selOrgId);
            listShowParameter.IsLookUp = true;
            listShowParameter.ListType = Convert.ToInt32(BOSEnums.Enu_ListType.BaseList);
            listShowParameter.ListFilterParameter.MergeFilter(e.ListFilterParameter);
            listShowParameter.IsShowUsed = false;
            listShowParameter.IsShowApproved = e.IsShowApproved;
            listShowParameter.FormId = FormId.ToString();
            listShowParameter.MultiSelect = false;
            listShowParameter.PageId = string.Format("{0}_{1}_F7", this.View.PageId, listShowParameter.FormId);
            this.View.ShowForm(listShowParameter, delegate (FormResult result)
            {
                object returnData = result.ReturnData;
                if (returnData is ListSelectedRowCollection)
                {
                    ListSelectedRowCollection listSelectedRowCollection = returnData as ListSelectedRowCollection;
                    if (listSelectedRowCollection != null && !listSelectedRowCollection.IsEmpty<ListSelectedRow>())
                    {
                        int row = e.Row;
                        if (key_Item.Trim() == "FBILLNO")
                        {
                            this.Model.SetValue(key_Field, listSelectedRowCollection[0].BillNo, row);
                        }
                        if (key_Item.Trim() == "FNUMBER")
                        {
                            this.Model.SetValue(key_Field, listSelectedRowCollection[0].Number, row);
                        }
                    }
                }
            });
        }
        private string BillNo(string id)
        {
            string result = string.Empty;
            if (id.Trim().Length > 0)
            {
                IQueryService service = ServiceFactory.GetService<IQueryService>(base.Context);
                QueryBuilderParemeter para = new QueryBuilderParemeter
                {
                    FormId = "SAL_SaleOrder",
                    SelectItems = SelectorItemInfo.CreateItems("FBILLNO,FID"),
                    FilterClauseWihtKey = string.Format(" FID = {0} ", id)
                };
                DynamicObjectCollection dynamicObjectCollection = service.GetDynamicObjectCollection(base.Context, para, null);
                result = dynamicObjectCollection[0]["FBILLNO"].ToString();
            }
            return result;
        }
        protected string GetSaleOrgFilter()
        {
            string empty = string.Empty;
            return string.Format(" FBizOrgId IN({0}) ", this.selOrgId);
        }
    }
}
