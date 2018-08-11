using Kingdee.BOS;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JFH.BDY.Contracts;
namespace JFH.BDY.ServiceHelper
{
    public static class CommonServiceHelper
    {

        public static long GetRoutingIdByMatAndAuxpro(Context ctx,long orgId, long materialId, string color)
        {
            ICommonService service = JHFServiceFactory.GetService<ICommonService>(ctx);
            try
            {
                return service.GetRoutingIdByMatAndAuxpro(ctx,orgId,materialId,color);
            }
            finally
            {
                JHFServiceFactory.CloseService(service);
            }
        }
    }
}
