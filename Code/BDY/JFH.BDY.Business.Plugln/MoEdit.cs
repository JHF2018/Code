using Kingdee.BOS.Core.Bill.PlugIn;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Kingdee.BOS.Core.DynamicForm.PlugIn.Args;
using Kingdee.BOS.Orm.DataEntity;
using JFH.BDY.ServiceHelper;
namespace JFH.BDY.Business.Plugln
{
    public class MoEdit: AbstractBillPlugIn
    {


        public override void DataChanged(DataChangedEventArgs e)
        {
            switch (e.Field.Key.ToUpperInvariant())
            {
                case "FMATERIALID":
                    break;
                case "FAUXPROPID":
              
                    //获取维度值 
                    string  newAuxpropData = e.NewValue as string;
                    if (string.IsNullOrWhiteSpace(newAuxpropData))
                    {
                        return;
                    }
                    DynamicObject org = this.View.Model.GetValue("FPrdOrgId") as DynamicObject;
                    long orgId = 0;
                    if (org != null)
                    {
                        orgId = Convert.ToInt64(org["Id"]);
                    }


                    DynamicObject material =   this.View.Model.GetValue("FMaterialId", e.Row) as DynamicObject;
                    long materialId = 0;
                    if (material!=null)
                    {
                        materialId = Convert.ToInt64(material["Id"]);
                    }
                    long routId = CommonServiceHelper.GetRoutingIdByMatAndAuxpro(this.Context, orgId,  materialId, newAuxpropData);
                    this.View.Model.SetValue("FRoutingId", routId, e.Row);
                    break;
                default:
                    break;
            }
            base.DataChanged(e);
        }
    }
}
