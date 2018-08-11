using Kingdee.BOS.Core.Metadata.ConvertElement.PlugIn;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Kingdee.BOS.Core.Metadata.ConvertElement.PlugIn.Args;
using Kingdee.BOS.Core.Metadata;
using Kingdee.BOS.Core;
using System.Data;
using Kingdee.BOS.Orm.DataEntity;
using Kingdee.BOS;
using Kingdee.BOS.App.Data;
using Kingdee.BOS.Contracts;
using Kingdee.BOS.Util;
using Kingdee.BOS.Orm.Metadata.DataEntity;
using Kingdee.BOS.Core.Metadata.FieldElement;
using Kingdee.BOS.Orm;

namespace JHF.BDY.App.ServicePlugIn.BillConvert
{
    public class RoutingDataTranferConvert : AbstractConvertPlugIn
    {


        public override void OnQueryBuilderParemeter(QueryBuilderParemeterEventArgs e)
        {
            //   e.SelectItems.Add(new SelectorItemInfo("FRoutingId"));
            e.SelectItems.Add(new SelectorItemInfo("FMaterialId"));
            e.SelectItems.Add(new SelectorItemInfo("FAuxPropId"));
            base.OnQueryBuilderParemeter(e);

        }

        public override void AfterConvert(AfterConvertEventArgs e)
        {

            ExtendedDataEntity[] entitys = e.Result.FindByEntityKey("FBillHead");
            DataTable dataTable = new DataTable();
            //dataTable.Columns.Add("FMaterialId");
            dataTable.Columns.Add("FCOLOR");
            List<long> lstAuxPropId = new List<long>();

            #region 第一步获取物料+辅助属性集合

            foreach (ExtendedDataEntity entity in entitys)
            {
                DynamicObjectCollection entryDatas = (DynamicObjectCollection)entity.DataEntity["TreeEntity"];
                if (entryDatas != null && entryDatas.Count > 0)
                {
                    foreach (var item in entryDatas)
                    {
                        DataRow dataRow = dataTable.NewRow();
                       // DynamicObject material = item["MaterialId"] as DynamicObject;

                        //dataRow["FMaterialId"] = material != null ? Convert.ToInt64(material["Id"]) : 0;
                        DynamicObject auxprop = item["AuxPropId"] as DynamicObject;
                        string color = auxprop != null ? Convert.ToString(auxprop["F100001_Id"]) : "";
                        dataRow["FCOLOR"] = color;
                        dataTable.Rows.Add(dataRow);

                    }
                }
            }

            if (dataTable.Rows.Count == 0)
            {
                return;
            }


            BatchSqlParam batchSqlParam = new BatchSqlParam("JHF_T_ROUTMATTAB", dataTable, "");
            batchSqlParam.TableAliases = "TR";
            // batchSqlParam.AddWhereExpression("FMaterialId", KDDbType.Int64, "F_JHF_MATERIALID", "TR");
            batchSqlParam.AddWhereExpression("FCOLOR", KDDbType.String, "F_JHF_COLOR", "TR");
            string selectFieldSql = " distinct  TR.F_JHF_COLOR,TR.F_JHF_ROUTID";
            string where = string.Format(" TR.FDOCUMENTSTATUS='C' AND TR.FFORBIDSTATUS='A'  ");


            DynamicObjectCollection dyobjs = DBUtils.ExecuteDynamicObject(base.Context, batchSqlParam, selectFieldSql, where);
            Dictionary<string, long> dicMatRout = new Dictionary<string, long>();
            if (dyobjs != null && dyobjs.Count > 0)
            {
                foreach (var objs in dyobjs)
                {
                    string color = Convert.ToString(objs["F_JHF_COLOR"]);
                    if (!dicMatRout.ContainsKey(color))
                    {
                        dicMatRout.Add(color, Convert.ToInt64(objs["F_JHF_ROUTID"]));
                    }
                }
            }



            #endregion

            #region 重新赋值工艺路线

            foreach (ExtendedDataEntity entity in entitys)
            {
                DynamicObjectCollection entryDatas = (DynamicObjectCollection)entity.DataEntity["TreeEntity"];
                if (entryDatas != null && entryDatas.Count > 0)
                {
                    foreach (var item in entryDatas)
                    {

                        DynamicObject material = item["MaterialId"] as DynamicObject;
                        string materialId = material != null ? Convert.ToString(material["Id"]) : "";
                        DynamicObject auxprop = item["AuxPropId"] as DynamicObject;
                        string color = auxprop != null ? Convert.ToString(auxprop["F100001_Id"]) : "";

                        if (dicMatRout.ContainsKey(color))
                        {
                            long rountId = dicMatRout[color];
                            //赋值工艺路线
                            SetRountIdFiled(e.TargetBusinessInfo, item, "RoutingId", "FRoutingId", rountId);

                        }

                    }
                }
            }
            #endregion
        }

        /// <summary>
        /// 赋值
        /// </summary>
        /// <param name="businessinfo"></param>
        /// <param name="dataEntity"></param>
        /// <param name="ormFieldKey"></param>
        /// <param name="fieldKey"></param>
        /// <param name="value"></param>
        private void SetRountIdFiled(BusinessInfo businessinfo, DynamicObject dataEntity, string ormFieldKey, string fieldKey, object value)
        {
            DynamicObject orgFieldData = (DynamicObject)dataEntity[ormFieldKey];
            if (orgFieldData == null)
            {
                BaseDataField baseField = businessinfo.GetField(fieldKey) as BaseDataField;
                SetBaseDataFieldValue(this.Context, baseField, dataEntity, value);
            }
        }

        public void SetBaseDataFieldValue(Context ctx, BaseDataField field, DynamicObject data, object value)
        {
            long result = 0;
            long.TryParse(value.ToString(), out result);
            if ((value.ToString().Length > 1) || (result > 0))
            {
                DynamicObject newValue = LoadReferenceData(ctx, field.RefFormDynamicObjectType, value);
                field.DynamicProperty.SetValue(data, newValue);
                field.RefIDDynamicProperty.SetValue(data, value);
            }
            else
            {
                field.DynamicProperty.SetValue(data, null);
                field.RefIDDynamicProperty.SetValue(data, 0);
            }
        }



        /// <summary>
        /// 加载引用属性
        /// </summary>
        /// <param name="ctx"></param>
        /// <param name="dt"></param>
        /// <param name="pkValue"></param>
        /// <returns></returns>
        private DynamicObject LoadReferenceData(Context ctx, DynamicObjectType dt, object pkValue)
        {
            if (pkValue.IsEmptyPrimaryKey())
            {
                return null;
            }
            OperateOption option = OperateOption.Create();
            option.SetThrowExceptionWhenNotFind(false);
            return Kingdee.BOS.ServiceHelper.BusinessDataServiceHelper.LoadSingle(ctx, pkValue, dt, option);

        }

        /// <summary>
        /// 加载引用属性
        /// </summary>
        /// <param name="ctx"></param>
        /// <param name="entryRows"></param>
        public void LoadReferenceData(Context ctx, ExtendedDataEntity[] entryRows)
        {
            IDBService dBService = ServiceFactory.GetDBService(ctx);
            dBService.LoadReferenceObject(ctx, (
                from w in entryRows
                select w.DataEntity).ToArray<DynamicObject>(), entryRows.FirstOrDefault<ExtendedDataEntity>().DataEntity.DynamicObjectType, true);
            ServiceFactory.CloseService(dBService);
        }

    }


}
