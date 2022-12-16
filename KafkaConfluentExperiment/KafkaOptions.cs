using System.ComponentModel.DataAnnotations;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaConfluentExperiment
{
    public  class KafkaOptions
    {
        [Required] public string BootstrapServers { get; set; } = default!;
        [Required] public string[] Topics { get; set; } = default!;
    }
}
