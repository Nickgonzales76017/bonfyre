import Foundation
import Vision

// Independent native decoder for a screenshot of the served public QR.
guard CommandLine.arguments.count == 2 else { exit(2) }
let request = VNDetectBarcodesRequest()
request.symbologies = [.qr]
let handler = VNImageRequestHandler(url: URL(fileURLWithPath: CommandLine.arguments[1]), options: [:])
try handler.perform([request])
guard let codes = request.results, codes.count == 1,
      let address = codes.first?.payloadStringValue else { exit(3) }
print(address)
