// bonfyre-detect-vision — what the detectors built into this machine find in an image.
//
// The detector the object-detection organ runs when no other one is named. It
// asks the system's own detectors for people, faces and animals and writes each
// finding with the box and confidence the detector reported. No model is
// downloaded and nothing leaves the machine.
//
//   bonfyre-detect-vision --image <file> --out <file.json> [--confidence 0.5]
//
// A box is a fraction of the image, measured from the top left. An image in
// which nothing was found yields an empty list; that is a finding, not a failure.
// An image that cannot be read is a failure and writes nothing.
import Foundation
import Vision
import CoreGraphics
import ImageIO

func fail(_ why: String, _ code: Int32 = 1) -> Never { FileHandle.standardError.write((why + "\n").data(using: .utf8)!); exit(code) }

struct Finding { let label: String; let confidence: Float; let box: CGRect }

var image: String?, out: String?, floor: Float = 0.5
var args = Array(CommandLine.arguments.dropFirst())
while !args.isEmpty {
    let flag = args.removeFirst()
    switch flag {
    case "--image": image = args.isEmpty ? nil : args.removeFirst()
    case "--out": out = args.isEmpty ? nil : args.removeFirst()
    case "--confidence": floor = args.isEmpty ? floor : (Float(args.removeFirst()) ?? floor)
    case "--model": if !args.isEmpty { args.removeFirst() }
    default: fail("usage: bonfyre-detect-vision --image <file> --out <file.json> [--confidence 0.5]", 2)
    }
}
guard let image, let out else { fail("usage: bonfyre-detect-vision --image <file> --out <file.json> [--confidence 0.5]", 2) }
let url = URL(fileURLWithPath: image)
guard let source = CGImageSourceCreateWithURL(url as CFURL, nil), let picture = CGImageSourceCreateImageAtIndex(source, 0, nil) else { fail("not a readable image: \(url.lastPathComponent)") }

let people = VNDetectHumanRectanglesRequest(), faces = VNDetectFaceRectanglesRequest(), animals = VNRecognizeAnimalsRequest()
do { try VNImageRequestHandler(cgImage: picture, options: [:]).perform([people, faces, animals]) }
catch { fail("detection did not run on \(url.lastPathComponent): \(error.localizedDescription)") }

var found: [Finding] = []
for seen in people.results ?? [] { found.append(Finding(label: "person", confidence: seen.confidence, box: seen.boundingBox)) }
for seen in faces.results ?? [] { found.append(Finding(label: "face", confidence: seen.confidence, box: seen.boundingBox)) }
for seen in animals.results ?? [] {
    guard let best = seen.labels.max(by: { $0.confidence < $1.confidence }) else { continue }
    found.append(Finding(label: best.identifier.lowercased(), confidence: best.confidence, box: seen.boundingBox))
}
found = found.filter { $0.confidence >= floor }.sorted { $0.confidence > $1.confidence }

func quoted(_ text: String) -> String {
    var s = "\""
    for scalar in text.unicodeScalars {
        switch scalar {
        case "\"": s += "\\\""
        case "\\": s += "\\\\"
        case "\n": s += "\\n"
        default: s += scalar.value < 0x20 ? String(format: "\\u%04x", scalar.value) : String(scalar)
        }
    }
    return s + "\""
}
let revision = "people-r\(VNDetectHumanRectanglesRequest.currentRevision)+faces-r\(VNDetectFaceRectanglesRequest.currentRevision)+animals-r\(VNRecognizeAnimalsRequest.currentRevision)"
var doc = "{\n  \"source\": \(quoted(image)),\n  \"model\": \(quoted("apple-vision/" + revision)),\n  \"detections\": [\n"
for (index, one) in found.enumerated() {
    // The detector measures from the bottom left; the organ's schema measures from the top left.
    let top = 1.0 - one.box.origin.y - one.box.height
    doc += String(format: "    {\"label\": %@, \"confidence\": %.3f, \"bbox\": {\"x\": %.3f, \"y\": %.3f, \"w\": %.3f, \"h\": %.3f}}%@\n",
                  quoted(one.label), one.confidence, one.box.origin.x, top, one.box.width, one.box.height, index < found.count - 1 ? "," : "")
}
doc += "  ]\n}\n"
do { try doc.write(toFile: out, atomically: true, encoding: .utf8) } catch { fail("cannot write \(out): \(error.localizedDescription)") }
