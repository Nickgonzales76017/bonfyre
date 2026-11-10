// BonfyreOCR — the text an image or a scanned page shows, with where each line sits.
//
// The operator the image and video pipeline recipes name at their OCR stage.
// It reads with the text recognition built into this machine: no model is
// downloaded, nothing leaves the machine, and nothing is installed.
//
//   bonfyre-ocr lines <image-or-pdf>     page, left, top, width, height, confidence, text (tab separated;
//                                        positions are fractions of the page, measured from the top left)
//   bonfyre-ocr text  <image-or-pdf>     the lines in reading order, one per line, pages separated by a form feed
//
// What it writes is what the recogniser read. A line's confidence is the
// recogniser's own; a high one is not proof the text is right.
import Foundation
import Vision
import CoreGraphics
import ImageIO
import PDFKit

func fail(_ why: String, _ code: Int32 = 1) -> Never { FileHandle.standardError.write((why + "\n").data(using: .utf8)!); exit(code) }

struct Line { let page: Int; let x, y, w, h: Double; let confidence: Float; let text: String }

func pages(_ url: URL) -> [CGImage] {
    if url.pathExtension.lowercased() == "pdf" {
        guard let doc = PDFDocument(url: url) else { fail("not a readable PDF: \(url.lastPathComponent)") }
        if doc.pageCount > 200 { fail("more than 200 pages; split the document") }
        return (0..<doc.pageCount).compactMap { index in
            guard let page = doc.page(at: index) else { return nil }
            let box = page.bounds(for: .mediaBox), scale = min(3.0, 2400.0 / max(box.width, box.height))
            let width = Int(box.width * scale), height = Int(box.height * scale)
            guard width > 0, height > 0, let ctx = CGContext(data: nil, width: width, height: height, bitsPerComponent: 8, bytesPerRow: 0, space: CGColorSpaceCreateDeviceRGB(), bitmapInfo: CGImageAlphaInfo.noneSkipLast.rawValue) else { return nil }
            ctx.setFillColor(CGColor(red: 1, green: 1, blue: 1, alpha: 1)); ctx.fill(CGRect(x: 0, y: 0, width: width, height: height))
            ctx.scaleBy(x: scale, y: scale); page.draw(with: .mediaBox, to: ctx)
            return ctx.makeImage()
        }
    }
    guard let source = CGImageSourceCreateWithURL(url as CFURL, nil), let image = CGImageSourceCreateImageAtIndex(source, 0, nil) else { fail("not a readable image: \(url.lastPathComponent)") }
    return [image]
}

func read(_ image: CGImage, page: Int) -> [Line] {
    let request = VNRecognizeTextRequest()
    request.recognitionLevel = .accurate
    request.usesLanguageCorrection = true
    do { try VNImageRequestHandler(cgImage: image, options: [:]).perform([request]) } catch { fail("the recogniser could not read page \(page): \(error.localizedDescription)", 3) }
    let found = (request.results ?? []).compactMap { seen -> Line? in
        guard let best = seen.topCandidates(1).first else { return nil }
        let b = seen.boundingBox       // Vision measures from the bottom left; lines are reported from the top left
        return Line(page: page, x: b.minX, y: 1 - b.maxY, w: b.width, h: b.height, confidence: best.confidence, text: best.string)
    }
    // Reading order: top to bottom, and left to right among lines that share a row.
    return found.sorted { a, b in abs(a.y - b.y) > min(a.h, b.h) * 0.6 ? a.y < b.y : a.x < b.x }
}

let args = CommandLine.arguments
guard args.count == 3, ["lines", "text"].contains(args[1]) else { fail("usage: bonfyre-ocr lines|text <image-or-pdf>", 2) }
let url = URL(fileURLWithPath: args[2])
guard FileManager.default.isReadableFile(atPath: url.path) else { fail("no such file: \(args[2])") }
let all = pages(url).enumerated().flatMap { read($0.element, page: $0.offset + 1) }
let clean = { (s: String) in s.replacingOccurrences(of: "\t", with: " ").replacingOccurrences(of: "\n", with: " ") }
if args[1] == "lines" {
    for l in all { print(String(format: "%d\t%.4f\t%.4f\t%.4f\t%.4f\t%.2f\t%@", l.page, l.x, l.y, l.w, l.h, l.confidence, clean(l.text))) }
} else {
    var page = 1
    for l in all { if l.page != page { print("\u{000C}", terminator: ""); page = l.page }; print(clean(l.text)) }
}
