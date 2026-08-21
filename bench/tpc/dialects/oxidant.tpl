-- Oxidant dialect for TPC-DS dsqgen: Netezza-style LIMIT (DataFusion-compatible)
-- plus `_END` required by the kit's template expander.
define __LIMITA = "";
define __LIMITB = "";
define __LIMITC = "limit %d";
define _END = "";
