select(.type == "deliver-result") |
  { crankNum, vatID, deliveryNum, compute: .dr[2].compute }
