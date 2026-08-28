import { z } from "zod";

export const MAX_U64 = 0xffff_ffff_ffff_ffffn;
const decimalU64Pattern = /^(0|[1-9][0-9]*)$/;

export const DecimalU64Schema = z
  .string()
  .max(20)
  .regex(decimalU64Pattern)
  .superRefine((value, context) => {
    if (value.length <= 20 && decimalU64Pattern.test(value) && BigInt(value) > MAX_U64) {
      context.addIssue({ code: "custom", message: "must fit in a uint64" });
    }
  });

export const PositiveDecimalU64Schema = DecimalU64Schema.refine(
  (value) => value !== "0",
  "must be a positive decimal u64",
);
