using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Demo.Common
{
    public class CalculationResponse
    {
        public int Result { get; set; }

        public override string ToString()
        {
            return Result.ToString();
        }
    }
}
