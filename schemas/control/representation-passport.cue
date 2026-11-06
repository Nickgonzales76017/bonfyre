// Compiled into dense runtime records; it never owns institutional truth.
package control

#RepresentationPassport: {
	identity: string & !=""
	subject_family: string & !=""
	semantic_digest: string & !=""
	representation_family: string & !=""
	representation_version: string & !=""
	exactness_class: "exact" | "latent"
	quality_loss_budget: number & >=0
	measurement_state: "measured" | "declared"
	shared_basis?: string
	binding_digest?: string
	exception_digest?: string
	physical_body_ref: string & !=""
	physical_digest: string & !=""
	supports: [...(string & !="")]
	lift_ref: string & !=""
	thaw_ref?: string
	bytes?: int & >=0
	encode_ns?: int & >=0
	decode_ns?: int & >=0
	scan_ns?: int & >=0
	random_read_ns?: int & >=0
	memory_bandwidth?: int & >=0
	copy_amplification?: number & >=0
	hydration_amplification?: number & >=0
	if exactness_class == "exact" { quality_loss_budget: 0 }
	if exactness_class == "latent" { thaw_ref: string & !="" }
}
