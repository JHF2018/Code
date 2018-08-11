using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JFH.BDY.Contracts;
using Kingdee.BOS.Rpc;
using System.ServiceModel;
using Kingdee.BOS;
namespace JFH.BDY.Contracts
{
    /// <summary>
    /// 公共服务接口
    /// </summary>
    [ServiceContract]
    [RpcServiceError]
    public   interface ICommonService
    {

        /// <summary>
        /// 普通新增的生产订单，从工艺路线物料颜色对照表获取相应的工艺路线返回填充
        /// </summary>
        /// <param name="ctx"></param>
        ///<param name="orgId"></param>
        /// <param name="materialId"></param>
        /// <param name="color"></param>
        /// <returns></returns>
        [OperationContract]
        [FaultContract(typeof(ServiceFault))]
        long GetRoutingIdByMatAndAuxpro(Context ctx,long orgId, long materialId, string color);

    }
}
