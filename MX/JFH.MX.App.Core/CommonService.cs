using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JFH.MX.Contracts;
using Kingdee.BOS;
using Kingdee.BOS.App;
using Kingdee.BOS.App.Data;
using Kingdee.BOS.Orm.DataEntity;

namespace JFH.MX.App.Core
{
    public class CommonService : ICommonService
    {
        /// <summary>
        /// 普通新增的生产订单，从工艺路线物料颜色对照表获取相应的工艺路线返回填充
        /// 客户需求不需要物料判定，暂时注释掉3333
        /// </summary>
        /// <param name="ctx"></param>
        ///<param name="orgId"></param>
        /// <param name="materialId"></param>
        /// <param name="color"></param>
        /// <returns></returns>
        public long GetRoutingIdByMatAndAuxpro(Context ctx, long orgId, long materialId, string color)
        {
            string sql = string.Format(@"select F_JHF_ROUTID  from JHF_T_ROUTMATTAB
                   where FDOCUMENTSTATUS='C' AND FFORBIDSTATUS='A'
          -- AND F_JHF_MATERIALID={0}
           and F_JHF_COLOR='{1}' and FUSEORGID={2}
          ", materialId, color,orgId);

        DynamicObject data = DBUtils.ExecuteDynamicObject(ctx, sql, null, null).FirstOrDefault();

            if (data != null)
            {
                return Convert.ToInt64(data["F_JHF_ROUTID"]);
            }




            return 0;




        }
    }
}
