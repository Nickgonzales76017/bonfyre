# Adds the "Your own model" demo pages to the work-samples site record from the
# saved runs in this folder. Rerun after a publish pulls the CMS copy.
import json, csv, os, sys
W=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
site=sys.argv[1]
d=json.load(open(site))
mon=json.load(open(f'{W}/monitor-apple.json'))
act=[m for m in mon['mentions'] if m['about']>=.5 and m['attention']>=.9]
verb={'respond':'Answer','risk':'Risk','opportunity':'Opening','awareness':'Look'}
mon_rows=[[m['title'][:90], verb[m['action']], m['publisher'], m['published']] for m in act]
aside=[[m['title'][:90], 'Set aside', m['publisher'], m['published']] for m in mon['mentions'] if m['about']<.5]
man=json.load(open(f'{W}/manifest-wholesale.json'))
man_rows=[[r['line'][:60] or r['source'], r['category'] or '—', '—' if r['data_bearing'] is None else ('yes' if r['data_bearing'] else 'no'), str(r['quantity'] or '—'), '; '.join(r['confirm_with_seller']) or '—'] for r in man['rows']]
books=list(csv.DictReader(open(f'{W}/books-september.csv')))
unsure=[b for b in books if b['decided_by']!='learned'][:10]
book_rows=[[b['description'][:60], b['suggested'], b['confidence'][:4], b['decided_by'], b['needs_review']] for b in unsure]
call={"kind":"call","title":"Want this for your business?","text":"Tell me what you need it to do. An introductory call is free.","button":"Send a project brief","href":"/#ask"}
def guide(path,url,name): return {"kind":"cases","id":path+"-guide","title":"Run it yourself","items":[{"title":name,"text":"The commands, the example files and the measured results, in the Bonfÿre guides.","icon":"menu_book","href":url,"button":"Open the guide"}]}
pages=[
 {"path":"own-model","title":"Work your own model does","menu":"Your own model","sections":[
  {"kind":"hero","headline":"A model on your own computer, *doing paid work.*","lede":"Three jobs a small model can do well when it is asked a narrow question and checked against what the text already says. Each runs on one computer, keeps a receipt for every answer, and sends nothing to an outside service.","button":"Discuss one of these","href":"/#ask"},
  {"kind":"cases","id":"own-model-jobs","title":"The three jobs","items":[
   {"title":"Mentions of a business","text":"What the news sources this installation reads said about a business, with namesakes set aside and the items that need doing something about listed first.","icon":"newspaper","href":"/mentions/","button":"See a real run"},
   {"title":"Books on your own machine","text":"Bank and card lines booked to a client's accounts, learning from earlier months, with the unsure lines held for review.","icon":"account_balance","href":"/books/","button":"See a real run"},
   {"title":"Lot manifests","text":"A seller's photos and listing turned into a manifest a buyer can price, with every gap listed for the seller to fill.","icon":"inventory_2","href":"/manifests/","button":"See a real run"}]},
  {"kind":"steps","id":"own-model-how","title":"How each job is built","items":[
   {"title":"The text settles what it can","text":"Names, model numbers, counts and stated conditions are copied as written. Rules decide what the words make certain, such as toner holding no data."},
   {"title":"The model answers a narrow question","text":"Is this the business or a namesake. Which of these accounts. What kind of equipment. Every answer keeps its probabilities and the exact model file that gave it."},
   {"title":"Unsure answers go to a person","text":"A low-confidence answer is held for review, and anything the seller or source did not state is listed to confirm."}]},
  call]},
 {"path":"mentions","title":"Mentions of a business","menu":"Mentions","sections":[
  {"kind":"hero","headline":"What the news said about a business, *namesakes set aside.*","lede":"bonfyre monitor reads the items an installation already collects, keeps the ones about the business, and lists first the few that call for a reply, carry a risk or offer an opening. This page shows a real run over 30 days of archived headlines naming Apple.","button":"Discuss this for your business","href":"/#ask"},
  {"kind":"figures","id":"mentions-act","title":"Listed first: what needs doing","columns":["Headline","Why","Source","Date"],"rows":mon_rows,
   "source":{"name":"bonfyre monitor over this installation's item archive, Qwen2.5 3B Instruct","observed":"16 October 2026"},
   "caveat":"40 headlines named Apple; 35 were about the company and 11 of those were listed here. A person had picked out four beforehand as needing action and all four are among them, with a possible import ban found afterwards."},
  {"kind":"figures","id":"mentions-aside","title":"Set aside: the name meant something else","columns":["Headline","Decision","Source","Date"],"rows":aside,
   "source":{"name":"bonfyre monitor, same run","observed":"16 October 2026"},"caveat":"On a separate run over 23 headlines containing the word Target, 22 were judged correctly as the retailer or not."},
  {"kind":"steps","id":"mentions-scope","title":"What is in it, and what is not","items":[
   {"title":"Working now","text":"Whole-name matching over the archive, one item per headline even when several feeds carry it, a judgement of business or namesake, the kind of mention, its tone and what to do, with a receipt for each."},
   {"title":"Not yet","text":"Local sources for a particular town. Coverage is the sources the installation reads, which are mostly national news."},
   {"title":"Nothing is sent","text":"The digest is a file. Replying, posting or contacting anyone stays with the business."}]},
  guide("mentions","https://docs.aurekai.work/business-mentions","Mentions of a business"), call]},
 {"path":"books","title":"Books on your own machine","menu":"Private books","sections":[
  {"kind":"hero","headline":"Bank lines booked to your accounts, *on your own computer.*","lede":"bonfyre books learns from the lines a client has already categorised and books a new month from that. The lines it is unsure of go to a local model, which sees how similar lines were booked before. Transactions never leave the computer.","button":"Discuss this for your business","href":"/#ask"},
  {"kind":"figures","id":"books-result","title":"One month of 1,500 lines","columns":["Outcome","Lines"],"rows":[["Booked from what earlier months taught","1,447"],["Booked by the local model","29"],["Held for review","24"],["Booked lines matching the given category","1,460 of 1,476"]],
   "source":{"name":"DoDataThings/us-bank-transaction-categories-v2, a public synthetic set of US bank lines (MIT licence); learned from 6,000 lines, tested on the next 1,500","url":"https://huggingface.co/DoDataThings/distilbert-us-transaction-classifier-v2","observed":"16 October 2026"},
   "caveat":"These are not anyone's real accounts. With no earlier months to learn from, every line goes to the local model and is held for review."},
  {"kind":"figures","id":"books-unsure","title":"Lines the learned weights were unsure of","columns":["Bank line","Suggested","Confidence","Decided by","Held for review"],"rows":book_rows,
   "source":{"name":"bonfyre books, same run","observed":"16 October 2026"}},
  {"kind":"steps","id":"books-scope","title":"What is in it, and what is not","items":[
   {"title":"Working now","text":"Learning from a categorised history, booking a bank or card export, a local model for the unsure lines, review for anything still unclear, and a receipt per line."},
   {"title":"Not yet","text":"Connecting to a bank or to the client's accounting software. Lines arrive as an export and leave as a CSV."},
   {"title":"The books stay theirs","text":"A booked line is a suggestion the client or their bookkeeper can change; a held line is not booked."}]},
  guide("books","https://docs.aurekai.work/books-on-your-machine","Books on your own machine"), call]},
 {"path":"manifests","title":"Lot manifests","menu":"Lot manifests","sections":[
  {"kind":"hero","headline":"A seller's photos and listing, *turned into a manifest.*","lede":"bonfyre manifest reads lot photos and listings on one computer, copies the make, model, part number and count exactly as written, and asks a local model what each unit is and whether it holds data to wipe. Anything the seller did not state is listed to confirm. The output loads into the ITAD acquisition desk.","button":"Discuss this for your business","href":"/#ask"},
  {"kind":"figures","id":"manifest-rows","title":"Seven lots from a real wholesale week","columns":["Read as","Kind","Data to wipe","Count","Ask the seller"],"rows":man_rows,
   "source":{"name":"Marketplace photos and two saved listings from the October 2026 wholesale pipeline, read by bonfyre manifest","observed":"16 October 2026"},
   "caveat":"The Dell row is a photo of toner boxes that was read as a desktop; the rest match what the photos and listings show."},
  {"kind":"steps","id":"manifest-scope","title":"What is in it, and what is not","items":[
   {"title":"Working now","text":"Reading photos and listings, exact brand, model, part number and count, kind of equipment, data to wipe, stated condition, and a file the ITAD acquisition desk imports."},
   {"title":"Not yet","text":"Counting units in a photo or reading a label too small to see. Marketplace photos rarely show labels, so most lots need the listing or close photos."}]},
  guide("manifests","https://docs.aurekai.work/lot-manifests","Lot manifests"), call]}]
d['pages']=[p for p in d['pages'] if p['path'] not in {x['path'] for x in pages}]+pages
nav=d['navigation']
if not any(n['href']=='/own-model/' for n in nav): nav.insert(1,{"label":"Your own model","href":"/own-model/"})
json.dump(d,open(site,'w'),indent=2,ensure_ascii=False); open(site,'a').write('\n')
print('pages', [p['path'] for p in d['pages']])
